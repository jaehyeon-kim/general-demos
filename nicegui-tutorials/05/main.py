from nicegui import ui

gender_image = ui.image(
    "https://raw.githubusercontent.com/turtlecode/python-nicegui/refs/heads/main/man.png"
).classes("w-48")

country_image = ui.image(
    "https://raw.githubusercontent.com/turtlecode/python-nicegui/refs/heads/main/england.png"
).classes("w-48")

toggle_gender = ui.toggle(
    ["man", "woman"],
    value="man",
    on_change=lambda: gender_image.set_source(
        f"https://raw.githubusercontent.com/turtlecode/python-nicegui/refs/heads/main/{toggle_gender.value}.png"
    ),
)

radio_country = ui.radio(
    ["england", "usa"],
    value="england",
    on_change=lambda: country_image.set_source(
        f"https://raw.githubusercontent.com/turtlecode/python-nicegui/refs/heads/main/{radio_country.value}.png"
    ),
)

ui.run()
