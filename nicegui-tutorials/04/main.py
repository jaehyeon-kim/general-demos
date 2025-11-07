from nicegui import ui

label = ui.label("")

toggle_text = ui.toggle(
    ["Text-1", "Text-2"],
    value="Text-1",
    on_change=lambda: label.set_text(f"You clicked {toggle_text.value}"),
)

image = ui.image("").classes("w-64")

toggle_image = ui.toggle(
    ["javascript", "python"],
    value="javascript",
    on_change=lambda: image.set_source(
        f"https://raw.githubusercontent.com/turtlecode/python-nicegui/refs/heads/main/{toggle_image.value}.png"
    ),
)

ui.run()
