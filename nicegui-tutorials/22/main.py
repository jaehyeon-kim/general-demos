from nicegui import ui

name = ui.input("Name")
age = ui.input("Age")

save = ui.button("Save", on_click=lambda: cb())


def cb():
    new_dict = {"name": name.value, "age": age.value}
    rows.append(new_dict)
    table.update_rows(rows)
    name.value = ""
    age.value = ""


columns = [{"label": "Name", "field": "name"}, {"label": "Age", "field": "age"}]
rows = []

table = ui.table(columns=columns, rows=rows)

ui.run()
