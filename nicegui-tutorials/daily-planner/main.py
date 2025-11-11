# https://github.com/nylas-samples/nicegui_planner/blob/python-v3/nicegui_planner.py
import random
from nicegui import ui
from datetime import date
from dataclasses import dataclass
from typing import List


@dataclass
class EventItems:
    name: str


@dataclass
class EmailItems:
    name: str


events: List[EventItems] = []
emails: List[EmailItems] = []


def update_events(current_date: str, _type: str) -> None:
    """Generates a random number of events or emails for a given date."""
    for i in range(random.randint(2, 5)):
        if _type == "event":
            events.append(EventItems(name=f"Event #{i + 1} @ {current_date}"))
        if _type == "email":
            emails.append(EmailItems(name=f"Email #{i + 1} @ {current_date}"))


def clear_events(_type: str) -> None:
    """Clears the event or email list."""
    if _type == "event":
        events.clear()
    if _type == "email":
        emails.clear()


def handle_input(e):
    """When we click on the calendar to select a new date, clear and update the lists."""
    current_date = str(date.today()) if e.value is None else e.value
    for _type in ["event", "email"]:
        clear_events(_type)
        update_events(current_date, _type)

    # Refresh the UI containers to show the new data
    event_ui.refresh()
    email_ui.refresh()


@ui.refreshable
def event_ui():
    """UI container for displaying the list of events."""
    ui.label("Events").classes("font-black text-4xl text-blue-700")
    if not events:
        ui.label("No events for this date.").classes("text-gray-500")
    for item in events:
        with ui.row().classes("items-center"):
            ui.label(item.name).classes("font-semibold text-lg")


@ui.refreshable
def email_ui():
    """UI container for displaying the list of emails."""
    ui.label("Emails").classes("font-black text-4xl text-blue-700")
    if not emails:
        ui.label("No emails for this date.").classes("text-gray-500")
    for item in emails:
        with ui.row().classes("items-center"):
            ui.label(item.name).classes("font-semibold text-lg")


# Main layout with everything centered
with ui.column().classes("w-full items-center gap-4"):
    today = date.today()
    current_date = str(today)

    # Set application title
    # Corrected: Use .classes() for styling
    ui.label("Daily Planner").classes("font-black text-6xl text-blue-700")

    # Create the calendar widget
    ui.date(value=today, on_change=handle_input)

    # Initial data generation for the first load
    for _type in ["event", "email"]:
        update_events(current_date, _type)

    # Render the initial UI for events and emails
    event_ui()
    email_ui()

# Run our application
ui.run(title="NiceGUI Planner")
