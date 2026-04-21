import re
from aiogram import Router, F
from aiogram.types import Message, CallbackQuery, PhotoSize
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.filters import Command
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.types import InlineKeyboardButton

from config import REGLAMENT, SUPPORT_USERNAME
from i18n import t
from database import (
    user_exists, register_user, get_user, set_agreed, set_lang, get_user_lang, is_banned,
    get_active_events, get_event, apply_to_event, cancel_application, has_applied,
    get_user_events, get_top_users, get_announcements, get_announcements_count,
    get_new_announcements_for_user, update_last_seen_ann,
    create_proposal, check_rate_limit,
    get_user_cards, set_user_photo,
    get_event_by_qr_token, confirm_attendance,
    recalc_streak
)
from keyboards import (
    main_menu_kb, agreement_kb, gender_kb,
    events_kb, event_detail_kb, announcements_nav_kb, cards_nav_kb
)

router = Router()


# ─── FSM ─────────────────────────────────────────────────────────────────────

class RegisterState(StatesGroup):
    lang       = State()
    full_name  = State()
    group_name = State()
    gender     = State()


class ProposalState(StatesGroup):
    vol_count      = State()
    location       = State()
    event_date     = State()
    duration       = State()
    tasks          = State()
    gender_need    = State()
    organizer      = State()
    admin_approved = State()
    comment        = State()


class PhotoState(StatesGroup):
    waiting = State()


# ─── Хелперы ─────────────────────────────────────────────────────────────────

def lang(tg_id: int) -> str:
    return get_user_lang(tg_id)


def main_menu_localized(tg_id: int):
    """Возвращает клавиатуру главного меню с локализованными кнопками."""
    from aiogram.utils.keyboard import ReplyKeyboardBuilder
    from aiogram.types import KeyboardButton
    l = lang(tg_id)
    builder = ReplyKeyboardBuilder()
    builder.row(KeyboardButton(text=t("btn_events", l)))
    builder.row(KeyboardButton(text=t("btn_profile", l)), KeyboardButton(text=t("btn_rating", l)))
    builder.row(KeyboardButton(text=t("btn_cards", l)),   KeyboardButton(text=t("btn_announce", l)))
    builder.row(KeyboardButton(text=t("btn_propose", l)), KeyboardButton(text=t("btn_support", l)))
    builder.row(KeyboardButton(text=t("btn_referral", l)),KeyboardButton(text=t("btn_howto", l)))
    builder.row(KeyboardButton(text=t("btn_lang", l)),    KeyboardButton(text=t("btn_home", l)))
    return builder.as_markup(resize_keyboard=True)


async def push_new_announcements(tg_id: int, bot):
    new_anns = get_new_announcements_for_user(tg_id)
    if not new_anns:
        return
    l = lang(tg_id)
    from datetime import datetime
    for ann in new_anns:
        try:
            dt = datetime.fromisoformat(ann["created_at"]).strftime("%d.%m.%Y")
            await bot.send_message(
                tg_id,
                f"📢 <b>{"Новое объявление SOV' if l=='ru' else 'Yangi e'lon SOV "if l=='uz' else "'New SOV announcement"}</b> ({dt})\n\n{ann['text']}",
                parse_mode="HTML"
            )
        except Exception:
            pass
    update_last_seen_ann(tg_id, new_anns[-1]["id"])


def lang_select_kb():
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(text="🇷🇺 Русский",   callback_data="setlang_ru"),
        InlineKeyboardButton(text="🇺🇿 O'zbek",     callback_data="setlang_uz"),
        InlineKeyboardButton(text="🇬🇧 English",    callback_data="setlang_en"),
    )
    return builder.as_markup()


# ─── /start ──────────────────────────────────────────────────────────────────

@router.message(Command("start"))
async def cmd_start(message: Message, state: FSMContext):
    tg_id = message.from_user.id

    # Проверяем реферальный параметр
    ref_id = None
    parts = message.text.split()
    if len(parts) > 1 and parts[1].startswith("ref"):
        try:
            ref_id = int(parts[1][3:])
            if ref_id == tg_id:
                ref_id = None
        except Exception:
            ref_id = None

    if user_exists(tg_id):
        user = get_user(tg_id)
        l    = user.get("lang", "ru")
        banned, ban_val = is_banned(tg_id)
        if banned:
            if ban_val == "full":
                await message.answer(t("banned_full", l))
            else:
                await message.answer(t("banned_temp", l, date=ban_val))
            return
        if not user.get("agreed"):
            await message.answer(
                t("read_agreement", l) + "\n\n" + REGLAMENT,
                parse_mode="HTML",
                reply_markup=agreement_kb(l)
            )
            return
        await message.answer(
            f"👋 {'С возвращением' if l=='ru' else 'Xush kelibsiz' if l=='uz' else 'Welcome back'}, {user['full_name']}!",
            reply_markup=main_menu_localized(tg_id)
        )
        await push_new_announcements(tg_id, message.bot)
        return

    # Новый пользователь — сначала выбор языка
    await state.update_data(ref_id=ref_id)
    await state.set_state(RegisterState.lang)
    await message.answer(
        "🌐 Выбери язык / Tilni tanlang / Choose language:",
        reply_markup=lang_select_kb()
    )


@router.callback_query(RegisterState.lang, F.data.startswith("setlang_"))
async def reg_lang(call: CallbackQuery, state: FSMContext):
    l = call.data.split("_")[1]
    await state.update_data(lang=l)
    await state.set_state(RegisterState.full_name)
    await call.message.edit_text(t("welcome_new", l), parse_mode="HTML")


@router.message(RegisterState.full_name)
async def reg_name(message: Message, state: FSMContext):
    data = await state.get_data()
    l    = data.get("lang", "ru")
    if not check_rate_limit(message.from_user.id, "register", 10, 60):
        await message.answer("⚠️ Too many requests. Wait a minute."); return
    name = message.text.strip()
    if len(name) < 5 or len(name) > 100:
        await message.answer("⚠️ 5–100 chars / символов / belgi"); return
    await state.update_data(full_name=name)
    await state.set_state(RegisterState.group_name)
    await message.answer(t("ask_group", l), parse_mode="HTML")


@router.message(RegisterState.group_name)
async def reg_group(message: Message, state: FSMContext):
    data = await state.get_data()
    l    = data.get("lang", "ru")
    group = message.text.strip()
    if len(group) < 2 or len(group) > 20:
        await message.answer("⚠️ 2–20 chars"); return
    await state.update_data(group_name=group)
    await state.set_state(RegisterState.gender)
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(text=t("gender_male", l),   callback_data="gender_М"),
        InlineKeyboardButton(text=t("gender_female", l), callback_data="gender_Ж"),
    )
    await message.answer(t("ask_gender", l), parse_mode="HTML", reply_markup=builder.as_markup())


@router.callback_query(RegisterState.gender, F.data.startswith("gender_"))
async def reg_gender(call: CallbackQuery, state: FSMContext):
    gender = call.data.split("_")[1]
    data   = await state.get_data()
    l      = data.get("lang", "ru")
    ref_id = data.get("ref_id")
    register_user(call.from_user.id, data["full_name"], data["group_name"], gender, l, ref_id)
    await state.clear()
    await call.message.edit_text(
        t("account_created", l, name=data["full_name"], group=data["group_name"], gender=gender),
        parse_mode="HTML"
    )
    await call.message.answer(
        t("read_agreement", l) + "\n\n" + REGLAMENT,
        parse_mode="HTML",
        reply_markup=agreement_kb(l)
    )


@router.callback_query(F.data == "agree_yes")
async def agree_handler(call: CallbackQuery):
    tg_id = call.from_user.id
    set_agreed(tg_id)
    l = lang(tg_id)
    await call.message.edit_text(t("agreed_ok", l), parse_mode="HTML")
    await call.message.answer(t("choose_section", l), reply_markup=main_menu_localized(tg_id))
    await push_new_announcements(tg_id, call.bot)


# ─── Выбор языка (в любой момент) ───────────────────────────────────────────

@router.message(F.func(lambda m: m.text and "Язык" in m.text or "Til" in m.text or "Language" in m.text))
async def change_lang_handler(message: Message):
    l = lang(message.from_user.id)
    await message.answer(t("choose_lang", l), reply_markup=lang_select_kb())


@router.callback_query(F.data.startswith("setlang_"))
async def set_lang_handler(call: CallbackQuery):
    l     = call.data.split("_")[1]
    tg_id = call.from_user.id
    set_lang(tg_id, l)
    await call.message.edit_text(t("lang_set", l), parse_mode="HTML")
    await call.message.answer(t("choose_section", l), reply_markup=main_menu_localized(tg_id))
    await call.answer()


# ─── Главное меню ────────────────────────────────────────────────────────────

@router.message(F.func(lambda m: m.text and ("Главное меню" in m.text or "Bosh menyu" in m.text or "Main menu" in m.text)))
async def main_menu_handler(message: Message):
    l = lang(message.from_user.id)
    await message.answer(t("choose_section", l), reply_markup=main_menu_localized(message.from_user.id))


# ─── Поддержка ───────────────────────────────────────────────────────────────

@router.message(F.func(lambda m: m.text and any(x in m.text for x in ["Поддержка", "Yordam", "Support"])))
async def support_handler(message: Message):
    l = lang(message.from_user.id)
    await message.answer(t("support_text", l, username=SUPPORT_USERNAME), parse_mode="HTML")


# ─── Как это работает ────────────────────────────────────────────────────────

@router.message(F.func(lambda m: m.text and any(x in m.text for x in ["работает", "ishlaydi", "How does"])))
async def howto_handler(message: Message):
    l = lang(message.from_user.id)
    await message.answer(t("howto_text", l), parse_mode="HTML")


# ─── Реферальная ссылка ──────────────────────────────────────────────────────

@router.message(F.func(lambda m: m.text and any(x in m.text for x in ["Реферал", "Referal", "Referral"])))
async def referral_handler(message: Message):
    tg_id = message.from_user.id
    l     = lang(tg_id)
    user  = get_user(tg_id)
    if not user:
        return
    bot_info = await message.bot.get_me()
    link  = f"https://t.me/{bot_info.username}?start=ref{tg_id}"
    count = user.get("referral_count", 0)
    await message.answer(t("referral_text", l, link=link, count=count), parse_mode="HTML")


# ─── Профиль ─────────────────────────────────────────────────────────────────

@router.message(F.func(lambda m: m.text and any(x in m.text for x in ["Мой профиль", "Mening profilim", "My profile"])))
async def my_profile(message: Message):
    tg_id = message.from_user.id
    if not user_exists(tg_id):
        await message.answer("Сначала /start"); return
    user   = get_user(tg_id)
    l      = user.get("lang", "ru")
    events = get_user_events(tg_id)

    gi = "♂" if user["gender"] == "М" else "♀"
    text = t("profile_header", l,
             gi=gi, name=user["full_name"], group=user["group_name"],
             rating=user["rating"], exp=user["experience"])

    pts = user.get("points", 0)
    if pts > 0:
        text += t("profile_points", l, pts=pts)

    streak = user.get("streak", 0)
    if streak >= 2:
        text += t("profile_streak", l, streak=streak)

    banned, ban_val = is_banned(tg_id)
    if banned:
        if ban_val == "full":
            text += t("profile_ban_full", l)
        else:
            text += t("profile_ban_temp", l, date=ban_val)

    if events:
        text += t("profile_history_header", l)
        icons = {"selected": "✅", "pending": "⏳", "rejected": "❌"}
        for ev in events[:5]:
            att = " 🎯" if ev.get("attended") else ""
            text += f"  {icons.get(ev['status'],'❓')} {ev['title']} ({ev['event_date']}){att}\n"

    # Кнопка смены фото
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text="📸 Сменить фото профиля" if l=="ru" else "📸 Profil rasmini o'zgartirish" if l=="uz" else "📸 Change profile photo",
                                     callback_data="change_photo"))
    kb = builder.as_markup()

    if user.get("photo_file_id"):
        await message.answer_photo(user["photo_file_id"], caption=text, parse_mode="HTML", reply_markup=kb)
    else:
        await message.answer(text, parse_mode="HTML", reply_markup=kb)


@router.callback_query(F.data == "change_photo")
async def change_photo_start(call: CallbackQuery, state: FSMContext):
    l = lang(call.from_user.id)
    await state.set_state(PhotoState.waiting)
    txt = {"ru": "📸 Отправь своё фото:", "uz": "📸 Rasmingizni yuboring:", "en": "📸 Send your photo:"}
    await call.message.answer(txt.get(l, txt["ru"]))
    await call.answer()


@router.message(PhotoState.waiting, F.photo)
async def save_photo(message: Message, state: FSMContext):
    photo: PhotoSize = message.photo[-1]
    set_user_photo(message.from_user.id, photo.file_id)
    await state.clear()
    l   = lang(message.from_user.id)
    txt = {"ru": "✅ Фото профиля обновлено!", "uz": "✅ Profil rasmi yangilandi!", "en": "✅ Profile photo updated!"}
    await message.answer(txt.get(l, txt["ru"]))


# ─── Рейтинг ─────────────────────────────────────────────────────────────────

@router.message(F.func(lambda m: m.text and any(x in m.text for x in ["Рейтинг", "Reyting", "Rating"])))
async def show_rating(message: Message):
    l   = lang(message.from_user.id)
    top = get_top_users(10)
    if not top:
        await message.answer("Пока нет данных."); return
    medals = ["🥇", "🥈", "🥉"]
    lines  = [
        f"{medals[i] if i<3 else str(i+1)+'.'} {u['full_name']} — ⭐{u['rating']} ({u['experience']})"
        for i, u in enumerate(top)
    ]
    header = {"ru": "🏆 <b>Рейтинг SOV</b>", "uz": "🏆 <b>SOV Reytingi</b>", "en": "🏆 <b>SOV Rating</b>"}
    await message.answer(header.get(l, header["ru"]) + "\n\n" + "\n".join(lines), parse_mode="HTML")


# ─── Объявления ──────────────────────────────────────────────────────────────

@router.message(F.func(lambda m: m.text and any(x in m.text for x in ["Объявления", "E'lonlar", "Announcements"])))
async def show_announcements(message: Message):
    tg_id = message.from_user.id
    l     = lang(tg_id)
    total = get_announcements_count()
    if total == 0:
        await message.answer(t("no_announcements", l)); return
    anns = get_announcements(limit=1, offset=0)
    ann  = anns[0]
    from datetime import datetime
    dt = datetime.fromisoformat(ann["created_at"]).strftime("%d.%m.%Y")
    await message.answer(
        t("announcement_header", l, date=dt, idx=1, total=total) + ann["text"],
        parse_mode="HTML",
        reply_markup=announcements_nav_kb(offset=0, total=total)
    )
    update_last_seen_ann(tg_id, ann["id"])


@router.callback_query(F.data.regexp(r"^ann_(next|prev|skip)_(\d+)$"))
async def ann_navigate(call: CallbackQuery):
    offset = int(call.data.split("_")[-1])
    total  = get_announcements_count()
    if offset < 0 or offset >= total:
        await call.answer(); return
    anns = get_announcements(limit=1, offset=offset)
    if not anns:
        await call.answer(); return
    ann = anns[0]
    l   = lang(call.from_user.id)
    from datetime import datetime
    dt = datetime.fromisoformat(ann["created_at"]).strftime("%d.%m.%Y")
    await call.message.edit_text(
        t("announcement_header", l, date=dt, idx=offset+1, total=total) + ann["text"],
        parse_mode="HTML",
        reply_markup=announcements_nav_kb(offset=offset, total=total)
    )
    await call.answer()


@router.callback_query(F.data == "ann_close")
async def ann_close(call: CallbackQuery):
    await call.message.delete()
    await call.answer()


# ─── Карточки ────────────────────────────────────────────────────────────────

@router.message(F.func(lambda m: m.text and any(x in m.text for x in ["Мои карточки", "Mening kartalarim", "My cards"])))
async def show_cards(message: Message):
    tg_id = message.from_user.id
    l     = lang(tg_id)
    cards = get_user_cards(tg_id)
    no_cards = {"ru": "🎴 У тебя пока нет карточек участника.", "uz": "🎴 Sizda hali ishtirokchi kartochkalari yo'q.", "en": "🎴 You have no participant cards yet."}
    if not cards:
        await message.answer(no_cards.get(l, no_cards["ru"])); return

    # Показываем первую карточку
    await _send_card(message.from_user.id, cards, 0, message)


async def _send_card(tg_id: int, cards: list, offset: int, message_or_call):
    l    = lang(tg_id)
    card = cards[offset]
    total = len(cards)
    from datetime import datetime
    issued = datetime.fromisoformat(card["issued_at"]).strftime("%d.%m.%Y")
    caption = (
        f"🎴 <b>{'Карточка участника' if l=='ru' else 'Ishtirokchi kartochkasi' if l=='uz' else 'Participant card'}</b>\n\n"
        f"📌 {card['title']}\n📅 {card['event_date']}\n"
        f"🏅 {'Выдана' if l=='ru' else 'Berilgan' if l=='uz' else 'Issued'}: {issued}\n\n"
        f"[{offset+1} / {total}]"
    )
    kb = cards_nav_kb(offset, total)
    if isinstance(message_or_call, Message):
        if card.get("photo_file_id"):
            await message_or_call.answer_photo(card["photo_file_id"], caption=caption, parse_mode="HTML", reply_markup=kb)
        else:
            await message_or_call.answer(caption, parse_mode="HTML", reply_markup=kb)
    else:
        call = message_or_call
        if card.get("photo_file_id"):
            await call.message.edit_caption(caption=caption, parse_mode="HTML", reply_markup=kb)
        else:
            await call.message.edit_text(caption, parse_mode="HTML", reply_markup=kb)


@router.callback_query(F.data.regexp(r"^card_(next|prev)_(\d+)$"))
async def card_navigate(call: CallbackQuery):
    offset = int(call.data.split("_")[-1])
    tg_id  = call.from_user.id
    cards  = get_user_cards(tg_id)
    if 0 <= offset < len(cards):
        await _send_card(tg_id, cards, offset, call)
    await call.answer()


@router.callback_query(F.data == "card_close")
async def card_close(call: CallbackQuery):
    await call.message.delete()
    await call.answer()


# ─── Ивенты ──────────────────────────────────────────────────────────────────

@router.message(F.func(lambda m: m.text and any(x in m.text for x in ["Активные ивенты", "Faol tadbirlar", "Active events"])))
async def list_events(message: Message):
    tg_id = message.from_user.id
    l     = lang(tg_id)
    banned, ban_val = is_banned(tg_id)
    if banned:
        await message.answer(t("banned_full", l) if ban_val == "full" else t("banned_temp", l, date=ban_val)); return

    user = get_user(tg_id)
    if not user or not user.get("agreed"):
        await message.answer("→ /start"); return

    events = get_active_events()
    if not events:
        await message.answer(t("no_events", l)); return

    applied_ids = {ev["id"] for ev in events if has_applied(ev["id"], tg_id)}
    await message.answer(t("events_header", l), parse_mode="HTML",
                         reply_markup=events_kb(events, applied_ids, l))


@router.callback_query(F.data.regexp(r"^event_(\d+)$"))
async def event_detail(call: CallbackQuery):
    event_id = int(call.data.split("_")[1])
    event    = get_event(event_id)
    if not event:
        await call.answer("Not found"); return

    tg_id   = call.from_user.id
    l       = lang(tg_id)
    user    = get_user(tg_id)
    app_status = has_applied(event_id, tg_id)
    already = app_status is not None

    slots_info = f"👥 {event['total_slots']}"
    if event["male_slots"] or event["female_slots"]:
        slots_info += f" (♂{event['male_slots']} / ♀{event['female_slots']})"

    can_apply  = True
    gender_note = ""
    if event.get("gender_strict"):
        m, f = event["male_slots"], event["female_slots"]
        if m > 0 and f == 0 and user["gender"] != "М":
            can_apply  = False
            gender_note = f"\n\n🔒 <i>{t('male_only', l)}</i>"
        if f > 0 and m == 0 and user["gender"] != "Ж":
            can_apply  = False
            gender_note = f"\n\n🔒 <i>{t('female_only', l)}</i>"

    loc  = f"\n📍 {event['location']}"   if event.get("location")     else ""
    time = f"\n🕐 {event['event_time']}" if event.get("event_time")   else ""
    dur  = f"\n⏱ {event['duration']}"   if event.get("duration")     else ""
    meet = f"\n🤝 {event['meeting_point']}" if event.get("meeting_point") else ""

    text = (
        f"📌 <b>{event['title']}</b>\n"
        f"📅 {event['event_date']}{time}{loc}{dur}{meet}\n"
        f"{slots_info}\n\n"
        f"{event['description'] or ''}") + gender_note

    if already:
        status_icons = {"selected": "✅", "pending": "⏳", "rejected": "❌"}
        
        # 1. Определяем текст статуса (без конфликта кавычек)
        if l == 'ru':
            status_text = "Ты записан"
        elif l == 'uz':
            status_text = "Ro'yxatdan o'tdingiz"
        else:
            status_text = "You are registered"

        # 2. Добавляем иконку и текст в переменную
        app_status = already.get("status", "pending") # Убедись, что app_status берется из записи
        icon = status_icons.get(app_status, '❓')
        text += f"\n\n{icon} <i>{status_text}</i>"

    # Важно: kb и дальнейший код должны быть ВНЕ блока 'if already' (на том же уровне, что и if)
    kb = event_detail_kb(event_id, already, can_apply, app_status, l)

    if event.get("photo_file_id"):
        try:
            await call.message.answer_photo(event["photo_file_id"], caption=text, parse_mode="HTML", reply_markup=kb)
            await call.message.delete()
        except Exception:
            await call.message.edit_text(text, parse_mode="HTML", reply_markup=kb)
    else:
        await call.message.edit_text(text, parse_mode="HTML", reply_markup=kb)


@router.callback_query(F.data.regexp(r"^apply_(\d+)$"))
async def apply_event(call: CallbackQuery):
    tg_id    = call.from_user.id
    l        = lang(tg_id)
    event_id = int(call.data.split("_")[1])
    banned, ban_val = is_banned(tg_id)
    if banned:
        await call.answer(t("banned_full", l) if ban_val=="full" else t("banned_temp", l, date=ban_val), show_alert=True); return
    if not check_rate_limit(tg_id, "apply", 5, 60):
        await call.answer("⚠️ Too many actions.", show_alert=True); return
    event = get_event(event_id)
    if not event or not event["is_active"]:
        await call.answer(t("event_closed", l), show_alert=True); return
    success, reason = apply_to_event(event_id, tg_id)
    if success:
        await call.answer(t("apply_success", l), show_alert=True)
        await event_detail(call)
    else:
        msg = {"male_only": t("male_only", l), "female_only": t("female_only", l),
               "already": t("already_applied", l)}.get(reason, reason)
        await call.answer(msg, show_alert=True)


@router.callback_query(F.data.regexp(r"^cancel_apply_(\d+)$"))
async def cancel_apply(call: CallbackQuery):
    tg_id    = call.from_user.id
    event_id = int(call.data.split("_")[2])
    cancel_application(event_id, tg_id)
    l = lang(tg_id)
    await call.answer("✅ " + ("Запись отменена" if l=="ru" else "Bekor qilindi" if l=="uz" else "Cancelled"), show_alert=True)
    await event_detail(call)


@router.callback_query(F.data == "back_events")
async def back_events(call: CallbackQuery):
    tg_id  = call.from_user.id
    l      = lang(tg_id)
    events = get_active_events()
    if not events:
        await call.message.edit_text(t("no_events", l)); return
    applied_ids = {ev["id"] for ev in events if has_applied(ev["id"], tg_id)}
    await call.message.edit_text(t("events_header", l), parse_mode="HTML",
                                  reply_markup=events_kb(events, applied_ids, l))


# ─── QR Сканирование ─────────────────────────────────────────────────────────

@router.message(Command("qr"))
async def qr_scan(message: Message):
    """Использование: /qr <token>"""
    tg_id = message.from_user.id
    l     = lang(tg_id)
    parts = message.text.split()
    if len(parts) < 2:
        await message.answer("Использование: /qr <token>"); return
    token = parts[1].strip()
    qr_row = get_event_by_qr_token(token)
    if not qr_row:
        await message.answer("❌ QR-код недействителен."); return
    event    = get_event(qr_row["event_id"])
    result   = confirm_attendance(qr_row["event_id"], tg_id)
    if result == "ok":
        await message.answer(t("qr_confirmed", l, event=event["title"]), parse_mode="HTML")
    elif result == "already":
        await message.answer(t("qr_already", l))
    else:
        await message.answer(t("qr_not_selected", l))


# ─── Предложить ивент ────────────────────────────────────────────────────────

@router.message(F.func(lambda m: m.text and any(x in m.text for x in ["Предложить ивент", "Tadbir taklif", "Propose event"])))
async def propose_start(message: Message, state: FSMContext):
    tg_id = message.from_user.id
    l     = lang(tg_id)
    if not user_exists(tg_id):
        await message.answer("→ /start"); return
    user = get_user(tg_id)
    if not user.get("agreed"):
        await message.answer("→ /start"); return
    banned, _ = is_banned(tg_id)
    if banned:
        await message.answer(t("banned_full", l)); return
    if not check_rate_limit(tg_id, "proposal", 3, 3600):
        await message.answer("⚠️ Слишком много предложений. Попробуй через час."); return

    steps = {
        "ru": "💡 <b>Предложение ивента</b>\n\nШаг 1/9 — Сколько волонтёров нужно?",
        "uz": "💡 <b>Tadbir taklifi</b>\n\n1/9-qadam — Nechta volontyor kerak?",
        "en": "💡 <b>Event proposal</b>\n\nStep 1/9 — How many volunteers are needed?",
    }
    await state.set_state(ProposalState.vol_count)
    await message.answer(steps.get(l, steps["ru"]), parse_mode="HTML")


@router.message(ProposalState.vol_count)
async def prop_vol(message: Message, state: FSMContext):
    text = message.text.strip()
    if not text.isdigit() or not (1 <= int(text) <= 200):
        await message.answer("⚠️ 1–200"); return
    await state.update_data(vol_count=text)
    await state.set_state(ProposalState.location)
    l = lang(message.from_user.id)
    steps = {"ru": "2/9 — 📍 Место проведения?", "uz": "2/9 — 📍 O'tkazish joyi?", "en": "2/9 — 📍 Location?"}
    await message.answer(steps.get(l, steps["ru"]))


@router.message(ProposalState.location)
async def prop_loc(message: Message, state: FSMContext):
    await state.update_data(location=message.text.strip())
    await state.set_state(ProposalState.event_date)
    l = lang(message.from_user.id)
    steps = {"ru": "3/9 — 📅 Когда? (дата и время)", "uz": "3/9 — 📅 Qachon? (sana va vaqt)", "en": "3/9 — 📅 When? (date and time)"}
    await message.answer(steps.get(l, steps["ru"]))


@router.message(ProposalState.event_date)
async def prop_date(message: Message, state: FSMContext):
    await state.update_data(event_date=message.text.strip())
    await state.set_state(ProposalState.duration)
    l = lang(message.from_user.id)
    steps = {"ru": "4/9 — ⏱ Длительность?", "uz": "4/9 — ⏱ Davomiylik?", "en": "4/9 — ⏱ Duration?"}
    await message.answer(steps.get(l, steps["ru"]))


@router.message(ProposalState.duration)
async def prop_dur(message: Message, state: FSMContext):
    await state.update_data(duration=message.text.strip())
    await state.set_state(ProposalState.tasks)
    l = lang(message.from_user.id)
    steps = {"ru": "5/9 — 📋 Что делают волонтёры?", "uz": "5/9 — 📋 Volontyorlar nima qiladi?", "en": "5/9 — 📋 What do volunteers do?"}
    await message.answer(steps.get(l, steps["ru"]))


@router.message(ProposalState.tasks)
async def prop_tasks(message: Message, state: FSMContext):
    await state.update_data(tasks=message.text.strip())
    await state.set_state(ProposalState.gender_need)
    l = lang(message.from_user.id)
    steps = {"ru": "6/9 — 🚻 Нужный пол? (Любой / Только парни / Только девушки)", "uz": "6/9 — 🚻 Kerakli jins?", "en": "6/9 — 🚻 Gender needed?"}
    await message.answer(steps.get(l, steps["ru"]))


@router.message(ProposalState.gender_need)
async def prop_gender(message: Message, state: FSMContext):
    await state.update_data(gender_need=message.text.strip())
    await state.set_state(ProposalState.organizer)
    l = lang(message.from_user.id)
    steps = {"ru": "7/9 — 👤 Кто организатор?", "uz": "7/9 — 👤 Tashkilotchi kim?", "en": "7/9 — 👤 Who is the organizer?"}
    await message.answer(steps.get(l, steps["ru"]))


@router.message(ProposalState.organizer)
async def prop_org(message: Message, state: FSMContext):
    await state.update_data(organizer=message.text.strip())
    await state.set_state(ProposalState.admin_approved)
    l = lang(message.from_user.id)
    steps = {"ru": "8/9 — 🏫 Одобрено администрацией лицея? (Да/Нет/На рассмотрении)", "uz": "8/9 — 🏫 Litsey ma'muriyati tomonidan tasdiqlangan?", "en": "8/9 — 🏫 Approved by school administration? (Yes/No/Pending)"}
    await message.answer(steps.get(l, steps["ru"]))


@router.message(ProposalState.admin_approved)
async def prop_admin(message: Message, state: FSMContext):
    await state.update_data(admin_approved=message.text.strip())
    await state.set_state(ProposalState.comment)
    l = lang(message.from_user.id)
    steps = {"ru": "9/9 — 💬 Комментарий для руководителя SOV (или «-»)", "uz": "9/9 — 💬 SOV rahbari uchun izoh (yoki «-»)", "en": "9/9 — 💬 Comment for SOV head (or «-»)"}
    await message.answer(steps.get(l, steps["ru"]))


@router.message(ProposalState.comment)
async def prop_comment(message: Message, state: FSMContext):
    comment = "" if message.text.strip() == "-" else message.text.strip()
    data    = await state.get_data()
    await state.clear()
    tg_id = message.from_user.id
    user  = get_user(tg_id)
    l     = user.get("lang", "ru")
    pid   = create_proposal(
        tg_id=tg_id, vol_count=data["vol_count"], location=data["location"],
        event_date=data["event_date"], duration=data["duration"], tasks=data["tasks"],
        gender_need=data["gender_need"], organizer=data["organizer"],
        admin_approved=data["admin_approved"], comment=comment
    )
    ok = {"ru": "✅ Предложение отправлено! Организатор рассмотрит его.", "uz": "✅ Taklif yuborildi!", "en": "✅ Proposal sent! The organizer will review it."}
    await message.answer(ok.get(l, ok["ru"]), parse_mode="HTML", reply_markup=main_menu_localized(tg_id))
    from config import ADMIN_IDS
    notif = (
        f"📬 <b>Новое предложение ивента #{pid}</b>\n"
        f"От: {user['full_name']} ({user['group_name']})\n\n"
        f"👥 {data['vol_count']} | 📍 {data['location']} | 📅 {data['event_date']}\n"
        f"⏱ {data['duration']} | 🚻 {data['gender_need']}\n"
        f"📋 {data['tasks']}\n👤 {data['organizer']}\n"
        f"🏫 {data['admin_approved']}\n💬 {comment or '—'}\n\n"
        f"/admin → 📬 Предложения"
    )
    for admin_id in ADMIN_IDS:
        try:
            await message.bot.send_message(admin_id, notif, parse_mode="HTML")
        except Exception:
            pass
