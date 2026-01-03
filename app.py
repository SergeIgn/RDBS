from sqlalchemy import (
    create_engine, Column, Integer, String, Date, Boolean,
    ForeignKey, Numeric, Text, CheckConstraint, Table
)
from sqlalchemy.orm import declarative_base, relationship, sessionmaker
from datetime import date

DATABASE_URL = "postgresql+psycopg2://postgres:G84832ABZ@localhost:5432/test1"

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(bind=engine)
Base = declarative_base()

items_authors = Table(
    "items_authors",
    Base.metadata,
    Column("id_item", ForeignKey("items.id_item"), primary_key=True),
    Column("id_author", ForeignKey("authors.id_author"), primary_key=True),
)

items_genres = Table(
    "items_genres",
    Base.metadata,
    Column("id_item", ForeignKey("items.id_item"), primary_key=True),
    Column("id_genre", ForeignKey("genres.id_genre"), primary_key=True),
)

class Author(Base):
    __tablename__ = "authors"

    id_author = Column(Integer, primary_key=True)
    first_name = Column(String(100), nullable=False)
    last_name = Column(String(100), nullable=False)
    birthdate = Column(Date)
    nationality = Column(String(100))

    items = relationship("Item", secondary=items_authors, back_populates="authors")

class Genre(Base):
    __tablename__ = "genres"

    id_genre = Column(Integer, primary_key=True)
    genre_name = Column(String(100), nullable=False)

    items = relationship("Item", secondary=items_genres, back_populates="genres")

class Member(Base):
    __tablename__ = "members"

    id_member = Column(Integer, primary_key=True)
    first_name = Column(String(100), nullable=False)
    last_name = Column(String(100), nullable=False)
    email = Column(String(255), unique=True)
    phone_number = Column(String(20), nullable=False)
    contact_address = Column(Text)
    add_date = Column(Date, nullable=False)

    __table_args__ = (
        CheckConstraint("add_date <= CURRENT_DATE", name="chk_member_add_date_past_or_today"),
    )

class Position(Base):
    __tablename__ = "positions"

    id_pos = Column(Integer, primary_key=True)
    title = Column(String(100), nullable=False)
    salary = Column(Numeric(10, 2), nullable=False)
    id_manager = Column(Integer, ForeignKey("positions.id_pos"))

    manager = relationship("Position", remote_side=[id_pos])
    employees = relationship("Employee", back_populates="position")

class Employee(Base):
    __tablename__ = "employees"

    id_employee = Column(Integer, primary_key=True)
    id_pos = Column(Integer, ForeignKey("positions.id_pos"), nullable=False)
    first_name = Column(String(100), nullable=False)
    last_name = Column(String(100), nullable=False)
    birthdate = Column(Date, nullable=False)
    email = Column(String(255), unique=True)
    phone_number = Column(String(20), nullable=False)
    contact_address = Column(Text, nullable=False)
    permanent_address = Column(Text)
    hired_date = Column(Date, nullable=False)
    hired_until = Column(Date)
    insurance = Column(Boolean)

    position = relationship("Position", back_populates="employees")

    __table_args__ = (
        CheckConstraint("birthdate < CURRENT_DATE", name="chk_employee_birthdate_past"),
    )

class Item(Base):
    __tablename__ = "items"

    id_item = Column(Integer, primary_key=True)
    title = Column(String(255), nullable=False)
    type = Column(String(50))
    publisher = Column(String(100))
    language = Column(String(50))
    code = Column(String(20), unique=True)
    pages = Column(Integer)

    authors = relationship("Author", secondary=items_authors, back_populates="items")
    genres = relationship("Genre", secondary=items_genres, back_populates="items")
    labels = relationship("Label", back_populates="item")

class Label(Base):
    __tablename__ = "labels"

    id_label = Column(Integer, primary_key=True)
    id_item = Column(Integer, ForeignKey("items.id_item"))
    status = Column(String(20), default="Available")

    item = relationship("Item", back_populates="labels")
    loans = relationship("Loan", back_populates="label")

class Loan(Base):
    __tablename__ = "labels_members_employees"

    id_loan = Column(Integer, primary_key=True)
    id_member = Column(Integer, ForeignKey("members.id_member"))
    id_employee = Column(Integer, ForeignKey("employees.id_employee"))
    id_label = Column(Integer, ForeignKey("labels.id_label"))
    loan_date = Column(Date, default=date.today, nullable=False)
    due_date = Column(Date, nullable=False)
    returned_date = Column(Date)

    member = relationship("Member")
    employee = relationship("Employee")
    label = relationship("Label")

    __table_args__ = (
        CheckConstraint(
            "returned_date IS NULL OR returned_date >= loan_date",
            name="chk_returned_after_loan",
        ),
    )

Base.metadata.create_all(engine)

session = SessionLocal()
# Select all items with authors
items = session.query(Item).all()
for i in items:
    print(i.title, [a.last_name for a in i.authors])
print("\n-----\n")
# Select all active loans
active_loans = session.query(Loan).filter(Loan.returned_date == None).all()
for loan in active_loans:
    print(f"Loan ID: {loan.id_loan}, Item: {loan.label.item.title}, Member: {loan.member.first_name} {loan.member.last_name}")
    print(f"    -> Label ID: {loan.id_label}, Due Date: {loan.due_date}")

print("\n-----\n")
# Join items and genres
results = (
    session.query(Item.title, Genre.genre_name)
    .join(Item.genres)
    .all()
)

for title, genre in results:
    print(f"<{title}> Genre: {genre}")

print("\n-----\n")
# Count records in each table and compute average
from sqlalchemy import select, func, literal
from sqlalchemy.orm import Session

session = Session(engine)

table_counts = (
    select(literal("Authors").label("table_name"), func.count(Author.id_author).label("row_count"))
    .union_all(
        select(literal("Genres"), func.count(Genre.id_genre)),
        select(literal("Members"), func.count(Member.id_member)),
        select(literal("Positions"), func.count(Position.id_pos)),
        select(literal("Employees"), func.count(Employee.id_employee)),
        select(literal("Items"), func.count(Item.id_item)),
        select(literal("Items_Authors"), func.count(items_authors.c.id_item)),
        select(literal("Items_Genres"), func.count(items_genres.c.id_item)),
        select(literal("Labels"), func.count(Label.id_label)),
        select(literal("Labels_Members_Employees"), func.count(Loan.id_loan)),
    )
).cte("TableCounts")

final_query = select(
    func.sum(table_counts.c.row_count).label("total_records"),
    func.count().label("total_tables"),
    func.avg(table_counts.c.row_count).label("average_records_per_table"),
)

result = session.execute(final_query).one()

print("Total records:", result.total_records)
print("Total tables:", result.total_tables)
print("Average records per table:", result.average_records_per_table)
session.close()

