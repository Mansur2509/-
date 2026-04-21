"""
Локализация бота SOV.
Используй t(key, lang, **kwargs) для получения текста.
"""

TEXTS = {
    # ── Регистрация ──────────────────────────────────────────────────────────
    "welcome_new": {
        "ru": "👋 Добро пожаловать в бот <b>SOV — School of Volunteers, Alwuit!</b>\n\nДавай создадим твой аккаунт.\n\n✏️ Введи своё <b>полное ФИО</b> на латинице:",
        "uz": "👋 <b>SOV — School of Volunteers, Alwuit</b> botiga xush kelibsiz!\n\nKeling, hisobingizni yarataylik.\n\n✏️ <b>To'liq ismingizni</b> kiriting (F.I.Sh.)ы:",
        "en": "👋 Welcome to the <b>SOV — School of Volunteers, Alwuit</b> bot!\n\nLet's create your account.\n\n✏️ Enter your <b>full name</b>:",
    },
    "ask_group": {
        "ru": "📚 Введи свою <b>группу</b> (например: 1rug7):",
        "uz": "📚 <b>Guruhingizni</b> kiriting (masalan: 1rug7):",
        "en": "📚 Enter your <b>group</b> (e.g. 1rug7):",
    },
    "ask_gender": {
        "ru": "👤 Выбери <b>пол</b>:",
        "uz": "👤 <b>Jinsingizni</b> tanlang:",
        "en": "👤 Choose your <b>gender</b>:",
    },
    "gender_male": {
        "ru": "♂ Мужской", "uz": "♂ Erkak", "en": "♂ Male",
    },
    "gender_female": {
        "ru": "♀ Женский", "uz": "♀ Ayol", "en": "♀ Female",
    },
    "account_created": {
        "ru": "✅ <b>Аккаунт создан!</b>\n\n👤 {name}\n📚 {group}\n🚻 {gender}",
        "uz": "✅ <b>Hisob yaratildi!</b>\n\n👤 {name}\n📚 {group}\n🚻 {gender}",
        "en": "✅ <b>Account created!</b>\n\n👤 {name}\n📚 {group}\n🚻 {gender}",
    },
    "read_agreement": {
        "ru": "Теперь прочитай и прими <b>Пользовательское соглашение SOV</b>:",
        "uz": "Endi SOV <b>foydalanuvchi shartnomasini</b> o'qing va qabul qiling:",
        "en": "Now read and accept the <b>SOV User Agreement</b>:",
    },
    "agree_btn": {
        "ru": "✅ Я согласен / Я согласна",
        "uz": "✅ Men roziman",
        "en": "✅ I agree",
    },
    "agreed_ok": {
        "ru": "✅ <b>Ты принял соглашение SOV.</b>\n\nДобро пожаловать! 🎉",
        "uz": "✅ <b>Siz SOV shartnomasini qabul qildingiz.</b>\n\nXush kelibsiz! 🎉",
        "en": "✅ <b>You accepted the SOV agreement.</b>\n\nWelcome! 🎉",
    },
    "choose_section": {
        "ru": "Выбери нужный раздел:",
        "uz": "Kerakli bo'limni tanlang:",
        "en": "Choose a section:",
    },
    # ── Главное меню ─────────────────────────────────────────────────────────
    "btn_events":    {"ru": "📋 Активные ивенты",    "uz": "📋 Faol tadbirlar",      "en": "📋 Active events"},
    "btn_profile":   {"ru": "👤 Мой профиль",        "uz": "👤 Mening profilim",     "en": "👤 My profile"},
    "btn_rating":    {"ru": "🏆 Рейтинг",            "uz": "🏆 Reyting",             "en": "🏆 Rating"},
    "btn_cards":     {"ru": "🎴 Мои карточки",       "uz": "🎴 Mening kartalarim",   "en": "🎴 My cards"},
    "btn_announce":  {"ru": "📢 Объявления",         "uz": "📢 E'lonlar",            "en": "📢 Announcements"},
    "btn_propose":   {"ru": "💡 Предложить ивент",   "uz": "💡 Tadbir taklif qilish","en": "💡 Propose event"},
    "btn_support":   {"ru": "🆘 Поддержка",          "uz": "🆘 Yordam",              "en": "🆘 Support"},
    "btn_home":      {"ru": "🏠 Главное меню",       "uz": "🏠 Bosh menyu",          "en": "🏠 Main menu"},
    "btn_referral":  {"ru": "🔗 Реферальная ссылка", "uz": "🔗 Referal havola",      "en": "🔗 Referral link"},
    "btn_howto":     {"ru": "📖 Как это работает?",  "uz": "📖 Bu qanday ishlaydi?", "en": "📖 How does it work?"},
    "btn_lang":      {"ru": "🌐 Язык / Til / Language", "uz": "🌐 Til / Язык / Language", "en": "🌐 Language / Язык / Til"},
    # ── Профиль ──────────────────────────────────────────────────────────────
    "profile_header": {
        "ru": "{gi} <b>{name}</b>\n📚 Группа: {group}\n⭐ Рейтинг: <b>{rating}</b>\n🎯 Ивентов: <b>{exp}</b>",
        "uz": "{gi} <b>{name}</b>\n📚 Guruh: {group}\n⭐ Reyting: <b>{rating}</b>\n🎯 Tadbirlar: <b>{exp}</b>",
        "en": "{gi} <b>{name}</b>\n📚 Group: {group}\n⭐ Rating: <b>{rating}</b>\n🎯 Events: <b>{exp}</b>",
    },
    "profile_ban_full": {
        "ru": "\n\n🚫 <b>Статус: Постоянный бан</b>",
        "uz": "\n\n🚫 <b>Holat: Doimiy ban</b>",
        "en": "\n\n🚫 <b>Status: Permanent ban</b>",
    },
    "profile_ban_temp": {
        "ru": "\n\n⏳ <b>Статус: Временный бан до {date}</b>",
        "uz": "\n\n⏳ <b>Holat: {date} gacha vaqtinchalik ban</b>",
        "en": "\n\n⏳ <b>Status: Temporary ban until {date}</b>",
    },
    "profile_points": {
        "ru": "\n⚠️ Поинты нарушений: <b>{pts}/3</b>",
        "uz": "\n⚠️ Jarima ballari: <b>{pts}/3</b>",
        "en": "\n⚠️ Violation points: <b>{pts}/3</b>",
    },
    "profile_history_header": {
        "ru": "\n\n📅 <b>Последние ивенты:</b>\n",
        "uz": "\n\n📅 <b>So'nggi tadbirlar:</b>\n",
        "en": "\n\n📅 <b>Recent events:</b>\n",
    },
    "profile_streak": {
        "ru": "\n🔥 Страйк: <b>{streak} ивент(ов) подряд</b>",
        "uz": "\n🔥 Streak: <b>{streak} tadbir ketma-ket</b>",
        "en": "\n🔥 Streak: <b>{streak} event(s) in a row</b>",
    },
    # ── Ивенты ───────────────────────────────────────────────────────────────
    "no_events": {
        "ru": "😔 Сейчас нет активных ивентов.",
        "uz": "😔 Hozirda faol tadbirlar yo'q.",
        "en": "😔 No active events right now.",
    },
    "events_header": {
        "ru": "📋 <b>Активные ивенты</b>\n\nВыбери ивент:",
        "uz": "📋 <b>Faol tadbirlar</b>\n\nTadbirni tanlang:",
        "en": "📋 <b>Active events</b>\n\nChoose an event:",
    },
    "apply_btn":      {"ru": "✋ Записаться",     "uz": "✋ Ro'yxatdan o'tish", "en": "✋ Sign up"},
    "cancel_btn":     {"ru": "❌ Отменить запись","uz": "❌ Bekor qilish",      "en": "❌ Cancel registration"},
    "back_btn":       {"ru": "◀️ Назад",          "uz": "◀️ Orqaga",           "en": "◀️ Back"},
    "apply_success":  {
        "ru": "✅ Ты успешно записался! Ожидай подтверждения.",
        "uz": "✅ Muvaffaqiyatli ro'yxatdan o'tdingiz! Tasdiqni kuting.",
        "en": "✅ Successfully signed up! Wait for confirmation.",
    },
    "already_applied":{"ru": "Ты уже подал заявку.", "uz": "Siz allaqachon ariza berdingiz.", "en": "You already applied."},
    "event_closed":   {"ru": "❌ Набор уже закрыт.", "uz": "❌ Ro'yxat yopilgan.", "en": "❌ Registration is closed."},
    "male_only":      {"ru": "❌ Этот ивент только для парней.", "uz": "❌ Bu tadbir faqat erkaklar uchun.", "en": "❌ This event is for males only."},
    "female_only":    {"ru": "❌ Этот ивент только для девушек.", "uz": "❌ Bu tadbir faqat ayollar uchun.", "en": "❌ This event is for females only."},
    # ── Объявления ───────────────────────────────────────────────────────────
    "no_announcements": {
        "ru": "📢 Объявлений пока нет.",
        "uz": "📢 Hozircha e'lonlar yo'q.",
        "en": "📢 No announcements yet.",
    },
    "announcement_header": {
        "ru": "📢 <b>Объявление</b> ({date})\n[{idx} из {total}]\n\n",
        "uz": "📢 <b>E'lon</b> ({date})\n[{idx} / {total}]\n\n",
        "en": "📢 <b>Announcement</b> ({date})\n[{idx} of {total}]\n\n",
    },
    # ── Поддержка ────────────────────────────────────────────────────────────
    "support_text": {
        "ru": "🆘 <b>Поддержка SOV</b>\n\nПо всем вопросам обращайся:\n{username}",
        "uz": "🆘 <b>SOV yordami</b>\n\nBarcha savollar uchun:\n{username}",
        "en": "🆘 <b>SOV Support</b>\n\nFor any questions contact:\n{username}",
    },
    # ── Реферал ──────────────────────────────────────────────────────────────
    "referral_text": {
        "ru": (
            "🔗 <b>Реферальная программа SOV</b>\n\n"
            "Пригласи друга в SOV и повысь свои шансы попасть на ивент!\n\n"
            "Твоя реферальная ссылка:\n<code>{link}</code>\n\n"
            "📌 За каждого приглашённого друга, который зарегистрируется и примет участие в ивенте, "
            "твой приоритет при отборе волонтёров повышается.\n\n"
            "👥 Приглашено тобой: <b>{count}</b> чел."
        ),
        "uz": (
            "🔗 <b>SOV referal dasturi</b>\n\n"
            "Do'stingizni SOV ga taklif qiling va tadbirga tushish imkoniyatingizni oshiring!\n\n"
            "Sizning referal havolangiz:\n<code>{link}</code>\n\n"
            "📌 Har bir taklif qilingan do'st uchun tanlov ustunligingiz oshadi.\n\n"
            "👥 Siz taklif qilganlar: <b>{count}</b> kishi."
        ),
        "en": (
            "🔗 <b>SOV Referral Program</b>\n\n"
            "Invite a friend to SOV and boost your chances of being selected!\n\n"
            "Your referral link:\n<code>{link}</code>\n\n"
            "📌 For each invited friend who registers and participates, your selection priority increases.\n\n"
            "👥 People invited by you: <b>{count}</b>"
        ),
    },
    # ── Инструкция ───────────────────────────────────────────────────────────
    "howto_text": {
        "ru": (
            "📖 <b>Как работает SOV Bot?</b>\n\n"
            "<b>1. Регистрация</b>\n"
            "Введи ФИО, группу и пол. Прими пользовательское соглашение.\n\n"
            "<b>2. Ивенты</b>\n"
            "Когда организаторы создают новый ивент — он появляется в «📋 Активные ивенты». "
            "Нажми «✋ Записаться» чтобы подать заявку.\n\n"
            "<b>3. Отбор</b>\n"
            "Есть два типа отбора: ручной и автоматический. Организатор может выбрать волонтёров самостоятельно или запустить автоподбор. Ручной способ понятен интуитивно, а вот как работает автоподбор?\n\n"
            "Организатор запускает автоподбор. Система выбирает волонтёров по рейтингу и опыту. "
            "Если тебя выбрали — придёт уведомление.\n\n"
            "<b>4. Напоминания</b>\n"
            "За 3 часа, 1 час и 15 минут до ивента бот напомнит тебе о нём.\n\n"
            "<b>5. Подтверждение участия</b>\n"
            "На ивенте сканируй QR-код организатора — это подтвердит твоё присутствие и даст тебе баллы.\n\n"
            "<b>6. Рейтинг</b>\n"
            "После ивента организатор выставит оценку (1–10). Средняя оценка = твой рейтинг. "
            "Чем выше рейтинг — тем больше шансов попасть на следующий ивент.\n\n"
            "<b>7. Штрафные поинты</b>\n"
            "За нарушения начисляются поинты (⚠️). 3 поинта = бан на месяц.\n\n"
            "<b>8. Топ-3</b>\n"
            "Каждый месяц бот объявляет трёх лучших волонтёров.\n\n"
            "<b>9. Карточки</b>\n"
            "За участие в ивентах ты получаешь карточки — цифровые сертификаты участника.\n\n"
            "❓ Вопросы? Нажми 🆘 Поддержка"
        ),
        "uz": (
            "📖 <b>SOV Bot qanday ishlaydi?</b>\n\n"
            "<b>1. Ro'yxatdan o'tish</b>\n"
            "F.I.Sh., guruh va jinsingizni kiriting. Foydalanuvchi shartnomasini qabul qiling.\n\n"
            "<b>2. Tadbirlar</b>\n"
            "Yangi tadbir yaratilganda u «📋 Faol tadbirlar» bo'limida paydo bo'ladi. "
            "Ariza berish uchun «✋ Ro'yxatdan o'tish» tugmasini bosing.\n\n"
            "<b>3. Tanlov</b>\n"
            "Saralashning ikki turi mavjud: qo'lda va avtomatik. Tashkilotchi ko'ngillilarni o'zi tanlashi yoki avtomatik saralashni ishga tushirishi mumkin. Qo'lda saralash usuli intuitiv tushunarli, ammo avtomatik saralash qanday ishlaydi?\n\n"
            "Tashkilotchi avtomatik tanlovni ishga tushiradi. Tizim reyting va tajribaga qarab tanlaydi. "
            "Siz tanlansangiz — xabarnoma keladi.\n\n"
            "<b>4. Eslatmalar</b>\n"
            "Tadbirdan 3 soat, 1 soat va 15 daqiqa oldin bot sizga eslatma yuboradi.\n\n"
            "<b>5. Ishtirokni tasdiqlash</b>\n"
            "Tadbirda tashkilotchining QR-kodini skanlang — bu sizning ishtirokingizni tasdiqlaydi.\n\n"
            "<b>6. Reyting</b>\n"
            "Tadbirdan so'ng tashkilotchi baho qo'yadi (1–10). O'rtacha baho = sizning reytingingiz.\n\n"
            "<b>7. Jarima ballari</b>\n"
            "Qoidabuzarlik uchun ball beriladi (⚠️). 3 ball = 1 oylik ban.\n\n"
            "<b>8. Top-3</b>\n"
            "Har oy eng yaxshi 3 ta volontyorni bot e'lon qiladi.\n\n"
            "❓ Savollar? 🆘 Yordam tugmasini bosing"
        ),
        "en": (
            "📖 <b>How does SOV Bot work?</b>\n\n"
            "<b>1. Registration</b>\n"
            "Enter your full name, group and gender. Accept the user agreement.\n\n"
            "<b>2. Events</b>\n"
            "When organizers create a new event, it appears in «📋 Active events». "
            "Press «✋ Sign up» to apply.\n\n"
            "<b>3. Selection</b>\n"
            "There are two types of selection: manual and automatic. The organizer can choose volunteers personally or launch the auto-selection tool. While the manual method is intuitive, how does the auto-selection actually work?\n\n"
            "The organizer runs auto-selection. The system picks volunteers by rating and experience. "
            "If selected — you'll get a notification.\n\n"
            "<b>4. Reminders</b>\n"
            "3 hours, 1 hour and 15 minutes before the event the bot will remind you.\n\n"
            "<b>5. Attendance confirmation</b>\n"
            "Scan the organizer's QR code at the event to confirm your attendance.\n\n"
            "<b>6. Rating</b>\n"
            "After the event the organizer gives a score (1–10). Average score = your rating.\n\n"
            "<b>7. Violation points</b>\n"
            "Rule violations earn points (⚠️). 3 points = 1 month ban.\n\n"
            "<b>8. Top-3</b>\n"
            "Every month the bot announces the top 3 volunteers.\n\n"
            "❓ Questions? Press 🆘 Support"
        ),
    },
    # ── Выбор языка ──────────────────────────────────────────────────────────
    "choose_lang": {
        "ru": "🌐 Выбери язык:",
        "uz": "🌐 Tilni tanlang:",
        "en": "🌐 Choose language:",
    },
    "lang_set": {
        "ru": "✅ Язык изменён на <b>Русский</b>",
        "uz": "✅ Til <b>O'zbek</b> tiliga o'zgartirildi",
        "en": "✅ Language changed to <b>English</b>",
    },
    # ── Бан ──────────────────────────────────────────────────────────────────
    "banned_full": {
        "ru": "🚫 Ты получил постоянный бан в SOV.\n\nОбратись к администратору.",
        "uz": "🚫 Siz SOV da doimiy ban oldingiz.\n\nAdministratorga murojaat qiling.",
        "en": "🚫 You have been permanently banned from SOV.\n\nContact the administrator.",
    },
    "banned_temp": {
        "ru": "⏳ Ты временно заблокирован до {date}.",
        "uz": "⏳ Siz {date} gacha vaqtinchalik bloklangansiz.",
        "en": "⏳ You are temporarily banned until {date}.",
    },
    # ── QR подтверждение ─────────────────────────────────────────────────────
    "qr_confirmed": {
        "ru": "✅ <b>Присутствие подтверждено!</b>\n\nИвент: <b>{event}</b>\n\nСпасибо за участие! 🎉",
        "uz": "✅ <b>Ishtirok tasdiqlandi!</b>\n\nTadbir: <b>{event}</b>\n\nIshtirok uchun rahmat! 🎉",
        "en": "✅ <b>Attendance confirmed!</b>\n\nEvent: <b>{event}</b>\n\nThank you for participating! 🎉",
    },
    "qr_already": {
        "ru": "ℹ️ Ты уже подтвердил участие в этом ивенте.",
        "uz": "ℹ️ Siz allaqachon bu tadbirda ishtirokingizni tasdiqlagansiz.",
        "en": "ℹ️ You already confirmed attendance for this event.",
    },
    "qr_not_selected": {
        "ru": "❌ Ты не в списке участников этого ивента.",
        "uz": "❌ Siz bu tadbir ishtirokchilari ro'yxatida emassiz.",
        "en": "❌ You are not on the participant list for this event.",
    },
}


def t(key: str, lang: str = "ru", **kwargs) -> str:
    """Получить перевод. Fallback на ru если ключ не найден."""
    entry = TEXTS.get(key, {})
    text  = entry.get(lang) or entry.get("ru") or f"[{key}]"
    if kwargs:
        try:
            text = text.format(**kwargs)
        except KeyError:
            pass
    return text
