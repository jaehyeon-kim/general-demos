from nicegui import ui

label = ui.label("Value: 0")
slider = (
    ui.slider(min=0, max=4, on_change=lambda: cb())
    .props("label-always")
    .classes("w-48")
)


def cb():
    label.set_text(f"Value: {slider.value}")
    if slider.value == 0:
        image.set_visibility(False)
    if slider.value == 1:
        image.set_visibility(True)
        image.set_source(
            "https://raw.githubusercontent.com/turtlecode/python-nicegui/refs/heads/main/basketball.jpg"
        )
    if slider.value == 2:
        image.set_visibility(True)
        image.set_source(
            "https://raw.githubusercontent.com/turtlecode/python-nicegui/refs/heads/main/football.jpg"
        )
    if slider.value == 3:
        image.set_visibility(True)
        image.set_source(
            "https://raw.githubusercontent.com/turtlecode/python-nicegui/refs/heads/main/tennis.jpg"
        )
    if slider.value == 4:
        image.set_visibility(False)
        slider.set_enabled(False)


image = ui.image("").classes("w-48")
image.set_visibility(False)

ui.run()
