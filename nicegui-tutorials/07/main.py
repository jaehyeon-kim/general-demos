from nicegui import ui

ui.button(
    "SAVE",
    on_click=lambda: cb(),
)


def cb():
    is_valid = True
    if cb_female.value and cb_male.value:
        ui.notify("You cannot choose both options.")
        is_valid = False
    if not cb_female.value and not cb_male.value:
        ui.notify("You must choose at least 1 option.")
        is_valid = False
    if cb_female.value and is_valid:
        gender_image.set_source(
            "https://raw.githubusercontent.com/turtlecode/python-nicegui/refs/heads/main/woman.png"
        )
    if cb_male.value and is_valid:
        gender_image.set_source(
            "https://raw.githubusercontent.com/turtlecode/python-nicegui/refs/heads/main/man.png"
        )


cb_male = ui.checkbox("male")
cb_female = ui.checkbox("female")


gender_image = ui.image("").classes("w-48")


ui.run()
