from nicegui import ui

switch_basketball = ui.switch("Basketball 🏀", on_change=lambda: cb())
switch_football = ui.switch("Football ⚽", on_change=lambda: cb())


def cb():
    if switch_basketball.value:
        # switch_football.set_value(False)
        sport_image.set_source(
            "https://raw.githubusercontent.com/turtlecode/python-nicegui/refs/heads/main/basketball.jpg"
        )
        label.set_text("I love Basketball!")
        sport_image.set_visibility(True)
        label.set_visibility(True)
    elif switch_football.value:
        # switch_basketball.set_value(False)
        sport_image.set_source(
            "https://raw.githubusercontent.com/turtlecode/python-nicegui/refs/heads/main/football.jpg"
        )
        label.set_text("I love Football!")
        sport_image.set_visibility(True)
        label.set_visibility(True)
    else:
        sport_image.set_source("")
        sport_image.set_visibility(False)
        label.set_visibility(False)


label = ui.label("")
label.set_visibility(False)
sport_image = ui.image("").classes("w-48")

ui.run()
