import sqlite3

connection = sqlite3.connect("example.db")
connection.execute(
    "CREATE TABLE IF NOT EXISTS books (title TEXT, author TEXT, year INTEGER)"
)
connection.execute(
    """
INSERT INTO books (title, author, year)
VALUES
    ('The Great Gatsby', 'F. Scott Fitzgerald', 1925),
    ('To Kill a Mockingbird', 'Harper Lee', 1960),
    ('1984', 'George Orwell', 1949),
    ('The Catcher in the Rye', 'J.D. Salinger', 1951)
"""
)
connection.commit()
connection.close()
