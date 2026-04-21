from aiogram.types import KeyboardButton, InlineKeyboardButton
from aiogram.utils.keyboard import ReplyKeyboardBuilder, InlineKeyboardBuilder
from i18n import t


# ─── Согласие ─────────────────────────────────────────────────────────────────

def agreement_kb(lang: str = "ru"):
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text=t("agree_btn", lang), callback_data="agree_yes"))
    return builder.as_markup()


# ─── Список ивентов ───────────────────────────────────────────────────────────

def events_kb(events: list, user_applications: set, lang: str = "ru"):
    builder = InlineKeyboardBuilder()
    for ev in events:
        applied = ev["id"] in user_applications
        label   = f"{'✅ ' if applied else ''}📌 {ev['title']} ({ev['event_date']})"
        builder.row(InlineKeyboardButton(text=label, callback_data=f"event_{ev['id']}"))
    return builder.as_markup()


def event_detail_kb(event_id: int, already_applied: bool, can_apply: bool = True,
                    app_status: str = None, lang: str = "ru"):
    builder = InlineKeyboardBuilder()
    if can_apply and not already_applied:
        builder.row(InlineKeyboardButton(text=t("apply_btn", lang), callback_data=f"apply_{event_id}"))
    if already_applied and app_status == "pending":
        builder.row(InlineKeyboardButton(text=t("cancel_btn", lang), callback_data=f"cancel_apply_{event_id}"))
    builder.row(InlineKeyboardButton(text=t("back_btn", lang), callback_data="back_events"))
    return builder.as_markup()


# ─── Карточки ─────────────────────────────────────────────────────────────────

def cards_nav_kb(offset: int, total: int):
    builder = InlineKeyboardBuilder()
    row = []
    if offset + 1 < total:
        row.append(InlineKeyboardButton(text="▶️", callback_data=f"card_next_{offset+1}"))
    if offset > 0:
        row.append(InlineKeyboardButton(text="◀️", callback_data=f"card_prev_{offset-1}"))
    if row:
        builder.row(*row)
    builder.row(InlineKeyboardButton(text="❌", callback_data="card_close"))
    return builder.as_markup()


# ─── Объявления ──────────────────────────────────────────────────────────────

def announcements_nav_kb(offset: int, total: int):
    builder = InlineKeyboardBuilder()
    row = []
    if offset + 3 < total:
        row.append(InlineKeyboardButton(text="⏫ +3", callback_data=f"ann_skip_{offset+3}"))
    if offset + 1 < total:
        row.append(InlineKeyboardButton(text="▶️", callback_data=f"ann_next_{offset+1}"))
    if offset > 0:
        row.append(InlineKeyboardButton(text="◀️", callback_data=f"ann_prev_{offset-1}"))
    if row:
        builder.row(*row)
    builder.row(InlineKeyboardButton(text="❌", callback_data="ann_close"))
    return builder.as_markup()


# ─── Главное меню (fallback, без локализации) ────────────────────────────────

def main_menu_kb():
    builder = ReplyKeyboardBuilder()
    builder.row(KeyboardButton(text="📋 Активные ивенты"))
    builder.row(KeyboardButton(text="👤 Мой профиль"), KeyboardButton(text="🏆 Рейтинг"))
    builder.row(KeyboardButton(text="🎴 Мои карточки"), KeyboardButton(text="📢 Объявления"))
    builder.row(KeyboardButton(text="💡 Предложить ивент"), KeyboardButton(text="🆘 Поддержка"))
    builder.row(KeyboardButton(text="🔗 Реферальная ссылка"), KeyboardButton(text="📖 Как это работает?"))
    builder.row(KeyboardButton(text="🌐 Язык / Til / Language"), KeyboardButton(text="🏠 Главное меню"))
    return builder.as_markup(resize_keyboard=True)


# ─── Админ-меню ──────────────────────────────────────────────────────────────

def admin_menu_kb():
    builder = ReplyKeyboardBuilder()
    builder.row(KeyboardButton(text="➕ Создать ивент"), KeyboardButton(text="⚡ Быстрый ивент из текста"))
    builder.row(KeyboardButton(text="📂 Все ивенты"), KeyboardButton(text="👥 Все участники"))
    builder.row(KeyboardButton(text="📢 Написать объявление"), KeyboardButton(text="📊 Топ-3 сейчас"))
    builder.row(KeyboardButton(text="📬 Предложения ивентов"))
    builder.row(KeyboardButton(text="🔙 Выйти из админки"))
    return builder.as_markup(resize_keyboard=True)


def admin_events_kb(events: list):
    builder = InlineKeyboardBuilder()
    for ev in events:
        status = "🟢" if ev["is_active"] else "🔴"
        builder.row(InlineKeyboardButton(
            text=f"{status} {ev['title']} ({ev['event_date']})",
            callback_data=f"adm_event_{ev['id']}"
        ))
    return builder.as_markup()


def admin_event_detail_kb(event_id: int, is_active: bool):
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text="🤖 Автоподбор",      callback_data=f"autoselect_{event_id}"))
    builder.row(InlineKeyboardButton(text="👥 Заявки",          callback_data=f"adm_apps_{event_id}"))
    builder.row(InlineKeyboardButton(text="⭐ Оценки",          callback_data=f"rate_event_{event_id}"))
    builder.row(InlineKeyboardButton(text="🎴 Выдать карточки", callback_data=f"issue_cards_{event_id}"))
    builder.row(InlineKeyboardButton(text="🔲 Генерировать QR", callback_data=f"gen_qr_{event_id}"))
    builder.row(InlineKeyboardButton(text="🖼 Загрузить фото",  callback_data=f"upload_img_{event_id}"))
    builder.row(
        InlineKeyboardButton(text="➕ Добавить участника", callback_data=f"manual_add_{event_id}"),
    )
    if is_active:
        builder.row(InlineKeyboardButton(text="🔒 Закрыть набор", callback_data=f"close_event_{event_id}"))
    builder.row(InlineKeyboardButton(text="🗑 Удалить ивент",   callback_data=f"del_event_{event_id}"))
    builder.row(InlineKeyboardButton(text="◀️ Назад",           callback_data="back_adm_events"))
    return builder.as_markup()


def admin_users_kb(users: list):
    builder = InlineKeyboardBuilder()
    for u in users:
        ban_icon = {"full": "🚫", "temp": "⏳"}.get(u["ban_type"], "")
        gi  = "♂" if u["gender"] == "М" else "♀"
        pts = f" ⚠️{u['points']}" if u.get("points", 0) > 0 else ""
        builder.row(InlineKeyboardButton(
            text=f"{ban_icon}{gi} {u['full_name']} ({u['group_name']}) ⭐{u['rating']}{pts}",
            callback_data=f"adm_user_{u['tg_id']}"
        ))
    return builder.as_markup()


def admin_user_detail_kb(tg_id: int, ban_type: str):
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(text="⚠️ +1 поинт", callback_data=f"pts_add_{tg_id}"),
        InlineKeyboardButton(text="✅ -1 поинт",  callback_data=f"pts_remove_{tg_id}"),
    )
    builder.row(InlineKeyboardButton(text="📋 История поинтов", callback_data=f"pts_history_{tg_id}"))
    if ban_type == "none":
        builder.row(
            InlineKeyboardButton(text="⏳ Бан 30д",  callback_data=f"ban_temp_{tg_id}"),
            InlineKeyboardButton(text="🚫 Фулл бан", callback_data=f"ban_full_{tg_id}"),
        )
    else:
        builder.row(InlineKeyboardButton(text="✅ Разбанить", callback_data=f"unban_{tg_id}"))
    builder.row(InlineKeyboardButton(text="📝 Заметка",           callback_data=f"edit_note_{tg_id}"))
    builder.row(InlineKeyboardButton(text="🗑 Удалить участника",  callback_data=f"del_user_{tg_id}"))
    builder.row(InlineKeyboardButton(text="◀️ Назад",              callback_data="back_adm_users"))
    return builder.as_markup()


def confirm_ban_kb(tg_id: int, ban_type: str):
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(text="✅ Подтвердить", callback_data=f"ban_confirm_{ban_type}_{tg_id}"),
        InlineKeyboardButton(text="❌ Отмена",       callback_data=f"adm_user_{tg_id}"),
    )
    return builder.as_markup()


def confirm_delete_kb(target: str, target_id: int):
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(text="🗑 Да, удалить", callback_data=f"del_confirm_{target}_{target_id}"),
        InlineKeyboardButton(text="❌ Отмена",       callback_data=f"del_cancel_{target}_{target_id}"),
    )
    return builder.as_markup()


def rate_select_user_kb(applications: list, event_id: int):
    builder = InlineKeyboardBuilder()
    for app in applications:
        if app["status"] == "selected":
            builder.row(InlineKeyboardButton(
                text=app["full_name"],
                callback_data=f"rate_user_{event_id}_{app['tg_id']}"
            ))
    builder.row(InlineKeyboardButton(text="◀️ Назад", callback_data=f"adm_event_{event_id}"))
    return builder.as_markup()


def proposals_kb(proposals: list):
    builder = InlineKeyboardBuilder()
    for p in proposals:
        builder.row(InlineKeyboardButton(
            text=f"📋 {p['full_name']} — {p['event_date']}",
            callback_data=f"proposal_{p['id']}"
        ))
    builder.row(InlineKeyboardButton(text="◀️ Закрыть", callback_data="close_proposals"))
    return builder.as_markup()


def proposal_action_kb(pid: int):
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(text="✅ Одобрить",  callback_data=f"prop_approve_{pid}"),
        InlineKeyboardButton(text="❌ Отклонить", callback_data=f"prop_reject_{pid}"),
    )
    builder.row(InlineKeyboardButton(text="◀️ Назад", callback_data="back_proposals"))
    return builder.as_markup()
# ─── Выбор пола (добавь это в конец файла) ────────────────────────────────────

def gender_kb(lang: str = "ru"):
    builder = InlineKeyboardBuilder()
    if lang == "ru":
        builder.row(
            InlineKeyboardButton(text="👨 Мужской", callback_data="set_gender_male"),
            InlineKeyboardButton(text="👩 Женский", callback_data="set_gender_female")
        )
    elif lang == "uz":
        builder.row(
            InlineKeyboardButton(text="👨 Erkak", callback_data="set_gender_male"),
            InlineKeyboardButton(text="👩 Ayol", callback_data="set_gender_female")
        )
    else:
        builder.row(
            InlineKeyboardButton(text="👨 Male", callback_data="set_gender_male"),
            InlineKeyboardButton(text="👩 Female", callback_data="set_gender_female")
        )
    return builder.as_markup()