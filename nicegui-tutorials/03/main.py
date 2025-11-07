from nicegui import ui

ui.button("Click me!", color="red", on_click=lambda: ui.label("You clicked me!"))

ui.button(
    "Show link!",
    color="blue",
    on_click=lambda: ui.link("Go to NiveGUI!!", "https://nicegui.io/", new_tab=True),
)

ui.button("Click me!", color="green", on_click=lambda: ui.notify("You clicked me!"))

with ui.button(
    "Click me!", color="pink", on_click=lambda: badge.set_text(int(badge.text) + 1)
):
    badge = ui.badge("0", color="red").props("floating")

ui.run()
