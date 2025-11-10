from nicegui import ui
from nicegui import app
from pathlib import Path

folder = Path(__file__).parent
files = [f.name for f in folder.glob("*.jpg")]
app.add_static_files("/static", folder)


def show(size):
    image_container.clear()
    with image_container:
        for filename in files:
            with ui.card():
                ui.image(f"/static/{filename}").style(f"width: {size}px")
                ui.label(filename)


ui.label(f"Current folder: {folder}")
with ui.row():
    ui.button("100px", on_click=lambda: show(100))
    ui.button("200px", on_click=lambda: show(200))
    ui.button("300px", on_click=lambda: show(300))
image_container = ui.row().classes("full flex items-center")

ui.run(title="Image Wall", favicon="📷")
