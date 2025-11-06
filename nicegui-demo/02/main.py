from nicegui import ui

ui.label("Welcome to NiceGUI!")

ui.link("Go to NiveGUI!!", "https://nicegui.io/", new_tab=True)

ui.chat_message(
    "Hello NiveGUI", name="Robot", stamp="now", avatar="https://robohash.org/ui"
)

ui.chat_message(
    "Hello NiveGUI", name="Robot", stamp="now", avatar="https://robohash.org/ui"
)

ui.markdown("This is **Markdown**.")

ui.mermaid(
    """
    graph LR;
        A --> B;
        A --> C;
    """
)

ui.html("This is <strong>HTML</strong>.", sanitize=False)

ui.run()
