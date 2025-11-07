from nicegui import ui

sports_image = ui.image(
    "https://raw.githubusercontent.com/turtlecode/python-nicegui/refs/heads/main/football.jpg"
).classes("w-48")

select_sports = ui.select(
    ["football", "hockey", "tennis"],
    label="Select a sport",
    value="football",
    on_change=lambda: sports_image.set_source(
        f"https://raw.githubusercontent.com/turtlecode/python-nicegui/refs/heads/main/{select_sports.value}.jpg"
    ),
).classes("w-48")


ui.run()
