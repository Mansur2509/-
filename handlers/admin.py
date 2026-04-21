import re
import io
import asyncio
import qrcode
from datetime import datetime

from aiogram import Router, F, Bot
from aiogram.types import Message, CallbackQuery, BufferedInputFile, PhotoSize
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.filters import Command
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.types import InlineKeyboardButton

from config import PAIRS_SCHEDULE
from database import (
    get_all_users, get_user, update_user_notes, delete_user,
    get_all_events, get_event, create_event, close_event, delete_event, set_event_photo,
    get_applications, auto_select, get_selected_for_event,
    manually_add_to_event, manually_remove_from_event,
    add_rating, get_top_users,
    add_points, get_point_history,
    ban_user, unban_user,
    create_announcement, get_all_users,
    get_proposals, update_proposal_status,
    issue_card, generate_qr_token,
    is_banned
)
from keyboards import (
    admin_menu_kb, admin_events_kb, admin_event_detail_kb,
    admin_users_kb, admin_user_detail_kb,
    rate_select_user_kb, main_menu_kb,
    confirm_ban_kb, confirm_delete_kb,
    proposals_kb, proposal_action_kb
)

router = Router()


def is_admin(tg_id: int) -> bool:
    from config import ADMIN_IDS
    return tg_id in ADMIN_IDS


# ─── Пары по расписанию ──────────────────────────────────────────────────────

def get_pair_info(time_str: str) -> str:
    if not time_str:
        return ""
    try:
        t = datetime.strptime(time_str.strip(), "%H:%M").time()
    except ValueError:
        return ""
    for start, end, label in PAIRS_SCHEDULE:
        ts = datetime.strptime(start, "%H:%M").time()
        te = datetime.strptime(end,   "%H:%M").time()
        if ts <= t <= te:
            return f"({label})"
    return ""


# ─── FSM ─────────────────────────────────────────────────────────────────────

class CreateEventState(StatesGroup):
    title         = State()
    description   = State()
    event_date    = State()
    event_time    = State()
    location      = State()
    duration      = State()
    meeting_point = State()
    total_slots   = State()
    male_slots    = State()
    female_slots  = State()
    gender_strict = State()


class QuickEventState(StatesGroup):
    raw_text = State()
    confirm  = State()


class RateState(StatesGroup):
    score   = State()
    comment = State()


class NoteState(StatesGroup):
    note = State()


class PointReasonState(StatesGroup):
    reason = State()


class AnnouncementState(StatesGroup):
    text = State()


class ManualAddState(StatesGroup):
    tg_id = State()


class UploadImgState(StatesGroup):
    photo = State()


# ─── Вход/выход ──────────────────────────────────────────────────────────────

@router.message(Command("admin"))
async def cmd_admin(message: Message):
    if not is_admin(message.from_user.id):
        await message.answer("⛔ Нет доступа."); return
    await message.answer("🔧 <b>Панель администратора SOV</b>",
                         parse_mode="HTML", reply_markup=admin_menu_kb())


@router.message(F.text == "🔙 Выйти из админки")
async def exit_admin(message: Message):
    await message.answer("Вышел.", reply_markup=main_menu_kb())


# ─── Быстрое создание ивента из текста ──────────────────────────────────────

def parse_event_text(raw: str) -> dict:
    """Извлекает дату, время, место, название из произвольного текста анонса."""
    result = {}

    # Заголовок — первая непустая строка
    lines = [l.strip() for l in raw.strip().split("\n") if l.strip()]
    if lines:
        result["title"] = lines[0][:100]

    # Дата: ищем "9 April", "April 9", "9.04", "09/04" и т.д.
    date_patterns = [
        r'\b(\d{1,2})\s+(January|February|March|April|May|June|July|August|September|October|November|December)\b',
        r'\b(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{1,2})\b',
        r'\b(\d{1,2})[./](\d{1,2})(?:[./](\d{2,4}))?\b',
    ]
    months_en = {"january":"01","february":"02","march":"03","april":"04","may":"05","june":"06",
                 "july":"07","august":"08","september":"09","october":"10","november":"11","december":"12"}
    for pat in date_patterns:
        m = re.search(pat, raw, re.IGNORECASE)
        if m:
            groups = m.groups()
            if groups[0].lower() in months_en:
                result["event_date"] = f"{groups[1]}.{months_en[groups[0].lower()]}"
            elif len(groups) > 1 and isinstance(groups[1], str) and groups[1].lower() in months_en:
                result["event_date"] = f"{groups[0]}.{months_en[groups[1].lower()]}"
            else:
                result["event_date"] = f"{groups[0]}.{groups[1] if len(groups)>1 else '??'}"
            break

    # Время: "11:30", "11.30", "at 10:00"
    time_m = re.search(r'\b(\d{1,2})[:\.](\d{2})\b', raw)
    if time_m:
        result["event_time"] = f"{time_m.group(1).zfill(2)}:{time_m.group(2)}"

    # Место: строка после "Venue:", "Location:", "Place:", "📍"
    venue_m = re.search(r'(?:Venue|Location|Place|📍)[:\s]+(.+)', raw, re.IGNORECASE)
    if venue_m:
        result["location"] = venue_m.group(1).strip()[:100]

    # Описание — весь текст
    result["description"] = raw[:800]
    return result


@router.message(F.text == "⚡ Быстрый ивент из текста")
async def quick_event_start(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id): return
    await state.set_state(QuickEventState.raw_text)
    await message.answer(
        "⚡ <b>Быстрое создание ивента</b>\n\n"
        "Скопируй и вставь текст объявления (с датой, временем, местом).\n"
        "Бот автоматически распознает данные:",
        parse_mode="HTML"
    )


@router.message(QuickEventState.raw_text)
async def quick_event_parse(message: Message, state: FSMContext):
    raw    = message.text.strip()
    parsed = parse_event_text(raw)
    await state.update_data(parsed=parsed, raw=raw)
    await state.set_state(QuickEventState.confirm)

    preview = (
        f"🔍 <b>Распознано:</b>\n\n"
        f"📌 Название: {parsed.get('title','—')}\n"
        f"📅 Дата: {parsed.get('event_date','—')}\n"
        f"🕐 Время: {parsed.get('event_time','—')}\n"
        f"📍 Место: {parsed.get('location','—')}\n\n"
        f"Создать ивент с этими данными?\n"
        f"<i>(потом можно отредактировать в базе через DB Browser)</i>"
    )
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(text="✅ Создать", callback_data="quick_confirm"),
        InlineKeyboardButton(text="❌ Отмена",  callback_data="quick_cancel"),
    )
    await message.answer(preview, parse_mode="HTML", reply_markup=builder.as_markup())


@router.callback_query(F.data == "quick_confirm")
async def quick_event_create(call: CallbackQuery, state: FSMContext):
    data   = await state.get_data()
    parsed = data.get("parsed", {})
    await state.clear()

    eid = create_event(
        title=parsed.get("title", "Новый ивент"),
        description=parsed.get("description", ""),
        event_date=parsed.get("event_date", "—"),
        event_time=parsed.get("event_time", ""),
        location=parsed.get("location", ""),
        duration="",
        meeting_point="",
        total_slots=10,
        male_slots=0,
        female_slots=0,
        gender_strict=0
    )
    await call.message.edit_text(
        f"✅ Ивент создан (ID: {eid})!\n\n"
        f"Уточни детали через <b>📂 Все ивенты</b> или отредактируй в DB Browser.",
        parse_mode="HTML"
    )
    await call.message.answer("⚙️", reply_markup=admin_menu_kb())
    await call.answer()


@router.callback_query(F.data == "quick_cancel")
async def quick_event_cancel(call: CallbackQuery, state: FSMContext):
    await state.clear()
    await call.message.edit_text("❌ Отменено.")
    await call.answer()


# ─── Создание ивента вручную ─────────────────────────────────────────────────

@router.message(F.text == "➕ Создать ивент")
async def create_ev_start(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id): return
    await state.set_state(CreateEventState.title)
    await message.answer("📌 <b>Название ивента:</b>", parse_mode="HTML")


@router.message(CreateEventState.title)
async def cev_title(message: Message, state: FSMContext):
    await state.update_data(title=message.text.strip())
    await state.set_state(CreateEventState.description)
    await message.answer("📝 <b>Описание</b> (или «-»):", parse_mode="HTML")


@router.message(CreateEventState.description)
async def cev_desc(message: Message, state: FSMContext):
    await state.update_data(description="" if message.text.strip()=="-" else message.text.strip())
    await state.set_state(CreateEventState.event_date)
    await message.answer("📅 <b>Дата</b> (например: 25.05.2025):", parse_mode="HTML")


@router.message(CreateEventState.event_date)
async def cev_date(message: Message, state: FSMContext):
    await state.update_data(event_date=message.text.strip())
    await state.set_state(CreateEventState.event_time)
    await message.answer("🕐 <b>Время начала</b> (например: 10:00, или «-»):", parse_mode="HTML")


@router.message(CreateEventState.event_time)
async def cev_time(message: Message, state: FSMContext):
    await state.update_data(event_time="" if message.text.strip()=="-" else message.text.strip())
    await state.set_state(CreateEventState.location)
    await message.answer("📍 <b>Место проведения</b> (или «-»):", parse_mode="HTML")


@router.message(CreateEventState.location)
async def cev_location(message: Message, state: FSMContext):
    await state.update_data(location="" if message.text.strip()=="-" else message.text.strip())
    await state.set_state(CreateEventState.duration)
    await message.answer("⏱ <b>Длительность</b> (или «-»):", parse_mode="HTML")


@router.message(CreateEventState.duration)
async def cev_duration(message: Message, state: FSMContext):
    await state.update_data(duration="" if message.text.strip()=="-" else message.text.strip())
    await state.set_state(CreateEventState.meeting_point)
    await message.answer("🤝 <b>Место встречи волонтёров</b> (или «-»):", parse_mode="HTML")


@router.message(CreateEventState.meeting_point)
async def cev_meeting(message: Message, state: FSMContext):
    await state.update_data(meeting_point="" if message.text.strip()=="-" else message.text.strip())
    await state.set_state(CreateEventState.total_slots)
    await message.answer("👥 Всего <b>волонтёров</b>:", parse_mode="HTML")


@router.message(CreateEventState.total_slots)
async def cev_total(message: Message, state: FSMContext):
    if not message.text.strip().isdigit():
        await message.answer("⚠️ Число."); return
    await state.update_data(total_slots=int(message.text.strip()))
    await state.set_state(CreateEventState.male_slots)
    await message.answer("♂ <b>Парней</b> (0 = без квоты):", parse_mode="HTML")


@router.message(CreateEventState.male_slots)
async def cev_male(message: Message, state: FSMContext):
    if not message.text.strip().isdigit():
        await message.answer("⚠️ Число."); return
    await state.update_data(male_slots=int(message.text.strip()))
    await state.set_state(CreateEventState.female_slots)
    await message.answer("♀ <b>Девушек</b> (0 = без квоты):", parse_mode="HTML")


@router.message(CreateEventState.female_slots)
async def cev_female(message: Message, state: FSMContext):
    if not message.text.strip().isdigit():
        await message.answer("⚠️ Число."); return
    await state.update_data(female_slots=int(message.text.strip()))
    await state.set_state(CreateEventState.gender_strict)
    await message.answer(
        "🔒 <b>Строгое ограничение по полу?</b> (Да/Нет)\n"
        "Если Да — другой пол не сможет записаться вообще.",
        parse_mode="HTML"
    )


@router.message(CreateEventState.gender_strict)
async def cev_strict(message: Message, state: FSMContext):
    strict = 1 if message.text.strip().lower() in ("да","yes","д","ha") else 0
    data   = await state.get_data()
    await state.clear()
    eid = create_event(
        title=data["title"], description=data["description"],
        event_date=data["event_date"], event_time=data["event_time"],
        location=data["location"], duration=data["duration"],
        meeting_point=data["meeting_point"],
        total_slots=data["total_slots"], male_slots=data["male_slots"],
        female_slots=data["female_slots"], gender_strict=strict
    )
    await message.answer(
        f"✅ Ивент <b>«{data['title']}»</b> создан!\n"
        f"📅 {data['event_date']} {data['event_time']}\n"
        f"📍 {data['location'] or '—'} | 👥 {data['total_slots']}\n"
        f"🔒 Строгий пол: {'Да' if strict else 'Нет'}\n\n"
        f"Загрузи фото ивента через кнопку «🖼 Загрузить фото» в списке ивентов.",
        parse_mode="HTML", reply_markup=admin_menu_kb()
    )


# ─── Все ивенты ──────────────────────────────────────────────────────────────

@router.message(F.text == "📂 Все ивенты")
async def all_events(message: Message):
    if not is_admin(message.from_user.id): return
    events = get_all_events()
    if not events:
        await message.answer("Ивентов нет."); return
    await message.answer("📂 <b>Все ивенты</b>:", parse_mode="HTML",
                         reply_markup=admin_events_kb(events))


@router.callback_query(F.data.regexp(r"^adm_event_(\d+)$"))
async def adm_event_detail(call: CallbackQuery):
    event_id = int(call.data.split("_")[2])
    event    = get_event(event_id)
    if not event:
        await call.answer("Не найдено."); return
    apps  = get_applications(event_id)
    sel   = sum(1 for a in apps if a["status"]=="selected")
    pend  = sum(1 for a in apps if a["status"]=="pending")
    text  = (
        f"📌 <b>{event['title']}</b>\n"
        f"📅 {event['event_date']} {event.get('event_time','')}\n"
        f"📍 {event.get('location','—')} | ⏱ {event.get('duration','—')}\n"
        f"🤝 {event.get('meeting_point','—')}\n"
        f"👥 {event['total_slots']} | 🔒 Строгий: {'Да' if event.get('gender_strict') else 'Нет'}\n"
        f"{'🟢 Открыт' if event['is_active'] else '🔴 Закрыт'} | 📨 {len(apps)} заявок (⏳{pend} ✅{sel})"
    )
    await call.message.edit_text(text, parse_mode="HTML",
                                  reply_markup=admin_event_detail_kb(event_id, bool(event["is_active"])))


@router.callback_query(F.data.regexp(r"^close_event_(\d+)$"))
async def adm_close_event(call: CallbackQuery):
    event_id = int(call.data.split("_")[2])
    close_event(event_id)
    await call.answer("🔒 Закрыт.", show_alert=True)
    await adm_event_detail(call)


@router.callback_query(F.data == "back_adm_events")
async def back_adm_events(call: CallbackQuery):
    events = get_all_events()
    await call.message.edit_text("📂 <b>Все ивенты</b>:", parse_mode="HTML",
                                  reply_markup=admin_events_kb(events))


# ─── Удаление ивента ─────────────────────────────────────────────────────────

@router.callback_query(F.data.regexp(r"^del_event_(\d+)$"))
async def del_event_confirm(call: CallbackQuery):
    event_id = int(call.data.split("_")[2])
    event    = get_event(event_id)
    await call.message.edit_text(
        f"🗑 Удалить ивент <b>«{event['title']}»</b>?\nВсе заявки и карточки будут удалены!",
        parse_mode="HTML",
        reply_markup=confirm_delete_kb("event", event_id)
    )


@router.callback_query(F.data.regexp(r"^del_confirm_event_(\d+)$"))
async def del_event_apply(call: CallbackQuery):
    event_id = int(call.data.split("_")[3])
    delete_event(event_id)
    await call.message.edit_text("🗑 Ивент удалён.")
    await call.answer()


# ─── Загрузка фото ивента ────────────────────────────────────────────────────

@router.callback_query(F.data.regexp(r"^upload_img_(\d+)$"))
async def upload_img_start(call: CallbackQuery, state: FSMContext):
    event_id = int(call.data.split("_")[2])
    await state.update_data(upload_event_id=event_id)
    await state.set_state(UploadImgState.photo)
    await call.message.answer("🖼 Отправь фото для ивента:")
    await call.answer()


@router.message(UploadImgState.photo, F.photo)
async def upload_img_save(message: Message, state: FSMContext):
    data     = await state.get_data()
    event_id = data["upload_event_id"]
    file_id  = message.photo[-1].file_id
    set_event_photo(event_id, file_id)
    await state.clear()
    await message.answer("✅ Фото ивента сохранено!", reply_markup=admin_menu_kb())


# ─── Заявки ──────────────────────────────────────────────────────────────────

@router.callback_query(F.data.regexp(r"^adm_apps_(\d+)$"))
async def adm_applications(call: CallbackQuery):
    event_id = int(call.data.split("_")[2])
    apps     = get_applications(event_id)
    if not apps:
        await call.answer("Заявок нет.", show_alert=True); return
    lines = []
    for a in apps:
        icon = {"selected":"✅","pending":"⏳","rejected":"❌"}.get(a["status"],"❓")
        gi   = "♂" if a["gender"]=="М" else "♀"
        att  = " 🎯" if a.get("attended") else ""
        lines.append(f"{icon}{gi} {a['full_name']} ({a['group_name']}) ⭐{a['rating']}{att}")
    await call.message.edit_text(
        "📨 <b>Заявки</b>\n\n" + "\n".join(lines), parse_mode="HTML",
        reply_markup=admin_event_detail_kb(event_id, True)
    )


# ─── Ручное добавление/удаление участника ────────────────────────────────────

@router.callback_query(F.data.regexp(r"^manual_add_(\d+)$"))
async def manual_add_start(call: CallbackQuery, state: FSMContext):
    event_id = int(call.data.split("_")[2])
    await state.update_data(manual_event_id=event_id)
    await state.set_state(ManualAddState.tg_id)
    await call.message.answer(
        "👤 Введи <b>Telegram ID</b> участника которого хочешь добавить:\n"
        "<i>(Узнать ID — попроси участника написать @userinfobot)</i>",
        parse_mode="HTML"
    )
    await call.answer()


@router.message(ManualAddState.tg_id)
async def manual_add_apply(message: Message, state: FSMContext, bot: Bot):
    data     = await state.get_data()
    event_id = data["manual_event_id"]
    try:
        tg_id = int(message.text.strip())
    except ValueError:
        await message.answer("⚠️ Введи числовой Telegram ID."); return
    user = get_user(tg_id)
    if not user:
        await message.answer("❌ Участник не найден в базе SOV."); return
    await state.clear()
    success = manually_add_to_event(event_id, tg_id)
    event   = get_event(event_id)
    if success:
        await message.answer(
            f"✅ <b>{user['full_name']}</b> добавлен на ивент «{event['title']}».",
            parse_mode="HTML", reply_markup=admin_menu_kb()
        )
        try:
            await bot.send_message(
                tg_id,
                f"🎉 Тебя добавили на ивент <b>«{event['title']}»</b> ({event['event_date']})!\n"
                f"📍 {event.get('location','')}\n🕐 {event.get('event_time','')}",
                parse_mode="HTML"
            )
        except Exception:
            pass
    else:
        await message.answer("❌ Не удалось добавить.")


@router.callback_query(F.data.regexp(r"^manual_remove_(\d+)_(\d+)$"))
async def manual_remove(call: CallbackQuery, bot: Bot):
    parts    = call.data.split("_")
    event_id = int(parts[2])
    tg_id    = int(parts[3])
    user     = get_user(tg_id)
    event    = get_event(event_id)
    manually_remove_from_event(event_id, tg_id)
    await call.answer(f"✅ {user['full_name']} удалён из ивента.", show_alert=True)
    try:
        await bot.send_message(
            tg_id,
            f"ℹ️ Тебя убрали из списка участников ивента «{event['title']}».",
            parse_mode="HTML"
        )
    except Exception:
        pass


# ─── Автоподбор + список для отпрашивания ────────────────────────────────────

@router.callback_query(F.data.regexp(r"^autoselect_(\d+)$"))
async def adm_autoselect(call: CallbackQuery, bot: Bot):
    event_id = int(call.data.split("_")[1])
    event    = get_event(event_id)
    result   = auto_select(event_id)
    selected = result["selected"]
    rejected = result["rejected"]
    if not selected:
        await call.answer("😔 Нет заявок.", show_alert=True); return

    pair_info = get_pair_info(event.get("event_time",""))
    time_str  = event.get("event_time","")
    pair_str  = f" {pair_info}" if pair_info else ""

    lines = [f"{i}. {s['full_name']} {s['group_name']}" for i, s in enumerate(selected, 1)]
    report = (
        f"📋 <b>Просьба отпросить волонтёров на ивент</b>\n"
        f"«<b>{event['title']}</b>»\n"
        f"📅 {event['event_date']} в {time_str}{pair_str}\n\n"
        + "\n".join(lines)
    )
    await call.message.answer(report, parse_mode="HTML")
    if rejected:
        rej_lines = [f"— {'♂' if r['gender']=='М' else '♀'} {r['full_name']}" for r in rejected]
        await call.message.answer(f"❌ <b>Не прошли:</b>\n" + "\n".join(rej_lines), parse_mode="HTML")
    await call.answer("✅ Готово!", show_alert=True)

    for s in selected:
        try:
            await bot.send_message(
                s["tg_id"],
                f"🎉 Ты выбран волонтёром на <b>«{event['title']}»</b>!\n"
                f"📅 {event['event_date']}" + (f" в {time_str}" if time_str else "")
                + (f"\n📍 {event.get('location','')}" if event.get("location") else "")
                + (f"\n🤝 Место встречи: {event.get('meeting_point','')}" if event.get("meeting_point") else "")
                + "\n\nОжидай напоминания перед ивентом.",
                parse_mode="HTML"
            )
        except Exception:
            pass


# ─── QR генерация ────────────────────────────────────────────────────────────

@router.callback_query(F.data.regexp(r"^gen_qr_(\d+)$"))
async def gen_qr(call: CallbackQuery, bot: Bot):
    event_id = int(call.data.split("_")[2])
    event    = get_event(event_id)
    token    = generate_qr_token(event_id)

    bot_info = await bot.get_me()
    qr_url   = f"https://t.me/{bot_info.username}?start=qrcheck_{token}"

    # Генерируем QR изображение
    img = qrcode.make(qr_url)
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    buf.seek(0)

    await call.message.answer_photo(
        BufferedInputFile(buf.getvalue(), filename="qr.png"),
        caption=(
            f"🔲 <b>QR-код для ивента «{event['title']}»</b>\n\n"
            f"Покажи этот QR волонтёрам на месте.\n"
            f"Они сканируют его камерой — откроется бот и подтвердит присутствие.\n\n"
            f"Или ссылка: <code>{qr_url}</code>"
        ),
        parse_mode="HTML"
    )
    await call.answer()


# ─── Выдача карточек ─────────────────────────────────────────────────────────

@router.callback_query(F.data.regexp(r"^issue_cards_(\d+)$"))
async def issue_cards(call: CallbackQuery, bot: Bot):
    event_id   = int(call.data.split("_")[2])
    volunteers = get_selected_for_event(event_id)
    event      = get_event(event_id)
    issued     = 0
    for vol in volunteers:
        if issue_card(vol["tg_id"], event_id):
            issued += 1
            try:
                await bot.send_message(
                    vol["tg_id"],
                    f"🎴 <b>Тебе выдана карточка участника!</b>\n\n"
                    f"Ивент: <b>«{event['title']}»</b> ({event['event_date']})\n\n"
                    f"Смотри карточки в разделе «🎴 Мои карточки».",
                    parse_mode="HTML"
                )
                await asyncio.sleep(0.05)
            except Exception:
                pass
    await call.answer(f"✅ Выдано карточек: {issued}", show_alert=True)


# ─── Участники ───────────────────────────────────────────────────────────────

@router.message(F.text == "👥 Все участники")
async def all_users_handler(message: Message):
    if not is_admin(message.from_user.id): return
    users = get_all_users()
    if not users:
        await message.answer("Участников нет."); return
    await message.answer("👥 <b>Все участники</b>:", parse_mode="HTML",
                         reply_markup=admin_users_kb(users))


@router.callback_query(F.data.regexp(r"^adm_user_(\d+)$"))
async def adm_user_detail(call: CallbackQuery):
    tg_id = int(call.data.split("_")[2])
    user  = get_user(tg_id)
    if not user:
        await call.answer("Не найден."); return
    gi  = "♂" if user["gender"]=="М" else "♀"
    pts = user.get("points",0)
    ban_status = "✅ Активен"
    if user["ban_type"] == "full":
        ban_status = "🚫 Постоянный бан"
    elif user["ban_type"] == "temp" and user["ban_until"]:
        bu = datetime.fromisoformat(user["ban_until"]).strftime("%d.%m.%Y")
        ban_status = f"⏳ Бан до {bu}"
    notes  = user["notes"] or "<i>Заметок нет</i>"
    streak = user.get("streak",0)
    lang   = user.get("lang","ru")
    ref_c  = user.get("referral_count",0)
    text = (
        f"{gi} <b>{user['full_name']}</b>\n"
        f"📚 {user['group_name']} | 🌐 {lang.upper()}\n"
        f"⭐ {user['rating']} | 🎯 {user['experience']} ив. | 🔥 Страйк: {streak}\n"
        f"⚠️ Поинты: <b>{pts}/3</b> | 🔗 Рефералов: {ref_c}\n"
        f"🔰 {ban_status}\n\n"
        f"📝 <b>Заметки:</b>\n{notes}\n\n"
        f"🆔 <code>{tg_id}</code>"
    )
    await call.message.edit_text(text, parse_mode="HTML",
                                  reply_markup=admin_user_detail_kb(tg_id, user["ban_type"]))


@router.callback_query(F.data == "back_adm_users")
async def back_adm_users(call: CallbackQuery):
    users = get_all_users()
    await call.message.edit_text("👥 <b>Все участники</b>:", parse_mode="HTML",
                                  reply_markup=admin_users_kb(users))


# ─── Удаление участника ──────────────────────────────────────────────────────

@router.callback_query(F.data.regexp(r"^del_user_(\d+)$"))
async def del_user_confirm(call: CallbackQuery):
    tg_id = int(call.data.split("_")[2])
    user  = get_user(tg_id)
    await call.message.edit_text(
        f"🗑 Удалить участника <b>{user['full_name']}</b>?\nВсе его данные будут удалены навсегда.",
        parse_mode="HTML",
        reply_markup=confirm_delete_kb("user", tg_id)
    )


@router.callback_query(F.data.regexp(r"^del_confirm_user_(\d+)$"))
async def del_user_apply(call: CallbackQuery):
    tg_id = int(call.data.split("_")[3])
    delete_user(tg_id)
    await call.message.edit_text("🗑 Участник удалён.")
    await call.answer()


@router.callback_query(F.data.regexp(r"^del_cancel_\w+_\d+$"))
async def del_cancel(call: CallbackQuery):
    await call.message.edit_text("❌ Удаление отменено.")
    await call.answer()


# ─── Поинты ──────────────────────────────────────────────────────────────────

@router.callback_query(F.data.regexp(r"^pts_add_(\d+)$"))
async def pts_add_start(call: CallbackQuery, state: FSMContext):
    tg_id = int(call.data.split("_")[2])
    await state.update_data(pts_tg_id=tg_id, pts_delta=1)
    await state.set_state(PointReasonState.reason)
    await call.message.answer("⚠️ Причина начисления поинта (или «-»):")
    await call.answer()


@router.callback_query(F.data.regexp(r"^pts_remove_(\d+)$"))
async def pts_remove_start(call: CallbackQuery, state: FSMContext):
    tg_id = int(call.data.split("_")[2])
    await state.update_data(pts_tg_id=tg_id, pts_delta=-1)
    await state.set_state(PointReasonState.reason)
    await call.message.answer("✅ Причина снятия поинта (или «-»):")
    await call.answer()


@router.message(PointReasonState.reason)
async def pts_apply(message: Message, state: FSMContext, bot: Bot):
    data   = await state.get_data()
    tg_id  = data["pts_tg_id"]
    delta  = data["pts_delta"]
    reason = "" if message.text.strip()=="-" else message.text.strip()
    await state.clear()
    result = add_points(tg_id, delta, reason)
    user   = get_user(tg_id)
    action = result["action"]
    action_text = {"temp_ban": "\n\n⏳ <b>Автоматически выдан бан на 30 дней!</b>",
                   "full_ban": "\n\n🚫 <b>Выдан постоянный бан!</b>"}.get(action, "")
    await message.answer(
        f"{'⚠️ +1' if delta>0 else '✅ -1'} поинт — <b>{user['full_name']}</b>\n"
        f"Поинтов: <b>{result['points']}/3</b>"
        + (f"\nПричина: {reason}" if reason else "") + action_text,
        parse_mode="HTML", reply_markup=admin_menu_kb()
    )
    try:
        if delta > 0:
            msg = (f"⚠️ Тебе начислен <b>предупредительный поинт</b>.\nПоинтов: <b>{result['points']}/3</b>"
                   + (f"\nПричина: {reason}" if reason else "")
                   + "\n\nПри 3 поинтах — бан на 30 дней.")
        else:
            msg = f"✅ С тебя снят поинт. Поинтов: <b>{result['points']}/3</b>"
        if action == "temp_ban": msg += "\n\n⏳ <b>Временный бан на 30 дней.</b>"
        elif action == "full_ban": msg += "\n\n🚫 <b>Постоянный бан в SOV.</b>"
        await bot.send_message(tg_id, msg, parse_mode="HTML")
    except Exception:
        pass


@router.callback_query(F.data.regexp(r"^pts_history_(\d+)$"))
async def pts_history(call: CallbackQuery):
    tg_id   = int(call.data.split("_")[2])
    user    = get_user(tg_id)
    history = get_point_history(tg_id)
    if not history:
        await call.answer("История пуста.", show_alert=True); return
    lines = []
    for h in history:
        dt   = datetime.fromisoformat(h["given_at"]).strftime("%d.%m %H:%M")
        sign = f"+{h['delta']}" if h["delta"]>0 else str(h["delta"])
        icon = "⚠️" if h["delta"]>0 else "✅"
        lines.append(f"{icon} {sign} — {dt}" + (f" ({h['reason']})" if h["reason"] else ""))
    await call.message.answer(
        f"📋 <b>История: {user['full_name']}</b>\n\n" + "\n".join(lines), parse_mode="HTML"
    )
    await call.answer()


# ─── Баны ────────────────────────────────────────────────────────────────────

@router.callback_query(F.data.regexp(r"^ban_(temp|full)_(\d+)$"))
async def ban_confirm_step(call: CallbackQuery):
    parts    = call.data.split("_")
    ban_type = parts[1]
    tg_id    = int(parts[2])
    user     = get_user(tg_id)
    label    = "временный бан (30д)" if ban_type=="temp" else "постоянный бан"
    await call.message.edit_text(
        f"⚠️ Выдать <b>{label}</b> для <b>{user['full_name']}</b>?",
        parse_mode="HTML", reply_markup=confirm_ban_kb(tg_id, ban_type)
    )


@router.callback_query(F.data.regexp(r"^ban_confirm_(temp|full)_(\d+)$"))
async def ban_apply(call: CallbackQuery, bot: Bot):
    parts    = call.data.split("_")
    ban_type = parts[2]
    tg_id    = int(parts[3])
    user     = get_user(tg_id)
    ban_user(tg_id, ban_type)
    label = "временный бан 30д" if ban_type=="temp" else "постоянный бан"
    await call.message.edit_text(f"✅ <b>{user['full_name']}</b> — {label}.", parse_mode="HTML")
    await call.answer()
    try:
        msg = "⏳ <b>Временный бан на 30 дней в SOV.</b>" if ban_type=="temp" else "🚫 <b>Постоянный бан в SOV.</b>"
        await bot.send_message(tg_id, msg, parse_mode="HTML")
    except Exception:
        pass


@router.callback_query(F.data.regexp(r"^unban_(\d+)$"))
async def unban_apply(call: CallbackQuery, bot: Bot):
    tg_id = int(call.data.split("_")[1])
    user  = get_user(tg_id)
    unban_user(tg_id)
    await call.message.edit_text(f"✅ Бан снят с <b>{user['full_name']}</b>.", parse_mode="HTML")
    await call.answer()
    try:
        await bot.send_message(tg_id, "✅ <b>Твой бан в SOV снят!</b>", parse_mode="HTML")
    except Exception:
        pass


# ─── Заметки ─────────────────────────────────────────────────────────────────

@router.callback_query(F.data.regexp(r"^edit_note_(\d+)$"))
async def edit_note_start(call: CallbackQuery, state: FSMContext):
    tg_id = int(call.data.split("_")[2])
    await state.update_data(target_tg_id=tg_id)
    await state.set_state(NoteState.note)
    await call.message.answer("📝 Новая заметка:")
    await call.answer()


@router.message(NoteState.note)
async def edit_note_save(message: Message, state: FSMContext):
    data = await state.get_data()
    update_user_notes(data["target_tg_id"], message.text.strip())
    await state.clear()
    await message.answer("✅ Заметка сохранена!", reply_markup=admin_menu_kb())


# ─── Оценки ──────────────────────────────────────────────────────────────────

@router.callback_query(F.data.regexp(r"^rate_event_(\d+)$"))
async def rate_event_start(call: CallbackQuery):
    event_id = int(call.data.split("_")[2])
    apps     = get_applications(event_id)
    if not any(a["status"]=="selected" for a in apps):
        await call.answer("Нет выбранных.", show_alert=True); return
    await call.message.edit_text("⭐ Выбери участника:",
                                  reply_markup=rate_select_user_kb(apps, event_id))


@router.callback_query(F.data.regexp(r"^rate_user_(\d+)_(\d+)$"))
async def rate_user_start(call: CallbackQuery, state: FSMContext):
    parts    = call.data.split("_")
    event_id = int(parts[2])
    tg_id    = int(parts[3])
    await state.update_data(rate_event_id=event_id, rate_tg_id=tg_id)
    await state.set_state(RateState.score)
    user = get_user(tg_id)
    await call.message.answer(f"⭐ Оцени <b>{user['full_name']}</b> (1–10):", parse_mode="HTML")
    await call.answer()


@router.message(RateState.score)
async def rate_score(message: Message, state: FSMContext):
    try:
        score = float(message.text.strip().replace(",","."))
        if not 1 <= score <= 10: raise ValueError
    except ValueError:
        await message.answer("⚠️ 1–10"); return
    await state.update_data(score=score)
    await state.set_state(RateState.comment)
    await message.answer("💬 Комментарий (или «-»):")


@router.message(RateState.comment)
async def rate_comment_save(message: Message, state: FSMContext, bot: Bot):
    data    = await state.get_data()
    comment = "" if message.text.strip()=="-" else message.text.strip()
    add_rating(data["rate_event_id"], data["rate_tg_id"], data["score"], comment)
    await state.clear()
    user  = get_user(data["rate_tg_id"])
    event = get_event(data["rate_event_id"])
    await message.answer(
        f"✅ Оценка <b>{data['score']}</b> — {user['full_name']}!",
        parse_mode="HTML", reply_markup=admin_menu_kb()
    )
    try:
        msg = f"📊 Оценка за <b>«{event['title']}»</b>: ⭐ {data['score']}/10"
        if comment: msg += f"\n💬 <i>{comment}</i>"
        msg += f"\n\n📈 Твой рейтинг: <b>{user['rating']}</b>"
        await bot.send_message(data["rate_tg_id"], msg, parse_mode="HTML")
    except Exception:
        pass


# ─── Объявления ──────────────────────────────────────────────────────────────

@router.message(F.text == "📢 Написать объявление")
async def ann_start(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id): return
    await state.set_state(AnnouncementState.text)
    await message.answer("📢 Текст объявления:")


@router.message(AnnouncementState.text)
async def ann_send(message: Message, state: FSMContext, bot: Bot):
    text = message.text.strip()
    await state.clear()
    create_announcement(text)
    users = get_all_users()
    sent  = 0
    for user in users:
        if not user.get("agreed"): continue
        banned, _ = is_banned(user["tg_id"])
        if banned: continue
        try:
            await bot.send_message(user["tg_id"], f"📢 <b>Объявление SOV</b>\n\n{text}", parse_mode="HTML")
            sent += 1
            await asyncio.sleep(0.05)
        except Exception:
            pass
    await message.answer(f"✅ Отправлено {sent} участникам.", reply_markup=admin_menu_kb())


# ─── Топ-3 ───────────────────────────────────────────────────────────────────

@router.message(F.text == "📊 Топ-3 сейчас")
async def top3_now(message: Message):
    if not is_admin(message.from_user.id): return
    top = get_top_users(3)
    if not top:
        await message.answer("Нет данных."); return
    medals = ["🥇","🥈","🥉"]
    lines  = [f"{medals[i]} <b>{u['full_name']}</b> ({u['group_name']})\n    ⭐{u['rating']} | 🎯{u['experience']}ив." for i,u in enumerate(top)]
    await message.answer("🏆 <b>Топ-3</b>\n\n" + "\n\n".join(lines), parse_mode="HTML")


# ─── Предложения ─────────────────────────────────────────────────────────────

@router.message(F.text == "📬 Предложения ивентов")
async def show_proposals(message: Message):
    if not is_admin(message.from_user.id): return
    props = get_proposals("pending")
    if not props:
        await message.answer("📬 Нет новых предложений."); return
    await message.answer(f"📬 <b>Предложения ({len(props)})</b>:", parse_mode="HTML",
                         reply_markup=proposals_kb(props))


@router.callback_query(F.data.regexp(r"^proposal_(\d+)$"))
async def proposal_detail(call: CallbackQuery):
    pid   = int(call.data.split("_")[1])
    all_p = get_proposals("pending") + get_proposals("approved") + get_proposals("rejected")
    p     = next((x for x in all_p if x["id"]==pid), None)
    if not p:
        await call.answer("Не найдено."); return
    text = (
        f"📋 <b>Предложение #{p['id']}</b> от {p['full_name']}\n\n"
        f"👥 {p['vol_count']} | 📍 {p['location']}\n📅 {p['event_date']} | ⏱ {p['duration']}\n"
        f"📋 {p['tasks']}\n🚻 {p['gender_need']}\n👤 {p['organizer']}\n"
        f"🏫 {p['admin_approved']}\n💬 {p['comment'] or '—'}"
    )
    await call.message.edit_text(text, parse_mode="HTML", reply_markup=proposal_action_kb(pid))


@router.callback_query(F.data.regexp(r"^prop_(approve|reject)_(\d+)$"))
async def proposal_action(call: CallbackQuery, bot: Bot):
    parts  = call.data.split("_")
    action = parts[1]
    pid    = int(parts[2])
    status = "approved" if action=="approve" else "rejected"
    update_proposal_status(pid, status)
    all_p = get_proposals("pending") + get_proposals("approved") + get_proposals("rejected")
    p     = next((x for x in all_p if x["id"]==pid), None)
    label = "✅ Одобрено" if action=="approve" else "❌ Отклонено"
    await call.message.edit_text(f"{label} предложение #{pid}.")
    await call.answer(label)
    if p:
        try:
            msg = (f"✅ Твоё предложение ивента одобрено!" if action=="approve"
                   else f"❌ Твоё предложение ивента отклонено.")
            await bot.send_message(p["tg_id"], msg + f"\n\nИвент: {p['event_date']}, {p['location']}")
        except Exception:
            pass


@router.callback_query(F.data == "back_proposals")
async def back_proposals(call: CallbackQuery):
    props = get_proposals("pending")
    await call.message.edit_text("📬 <b>Предложения</b>:", parse_mode="HTML", reply_markup=proposals_kb(props))


@router.callback_query(F.data == "close_proposals")
async def close_proposals(call: CallbackQuery):
    await call.message.delete()
    await call.answer()
