-- Books table
CREATE TABLE books (
    book_id INT PRIMARY KEY,
    title VARCHAR(255),
    author VARCHAR(255),
    genre VARCHAR(100),
    publication_year INT
);

-- Members table
CREATE TABLE members (
    member_id INT PRIMARY KEY,
    name VARCHAR(255),
    contact VARCHAR(100),
    membership_status VARCHAR(50)
);

-- Loans table
CREATE TABLE loans (
    loan_id INT PRIMARY KEY,
    book_id INT REFERENCES books(book_id),
    member_id INT REFERENCES members(member_id),
    loan_date DATE,
    due_date DATE,
    return_date DATE
);
