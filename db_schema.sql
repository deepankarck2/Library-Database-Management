DROP TABLE IF EXISTS Prices;
CREATE TABLE Prices (
  price_id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
  rental_price DECIMAL(5,2) NOT NULL CHECK (rental_price >= 0)
);


DROP TABLE IF EXISTS Books;
CREATE TABLE Books (
  book_id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
  title VARCHAR(255) NOT NULL,
  author VARCHAR(255) NOT NULL,
  genre VARCHAR(255),
  publication_year YEAR,
  price_id INT,
  FOREIGN KEY (price_id) REFERENCES Prices(price_id) ON UPDATE CASCADE ON DELETE SET NULL
);


DROP TABLE IF EXISTS Rentals;
CREATE TABLE Rentals (
  rental_id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
  book_id INT,
  member_id INT NOT NULL,
  rental_date DATE NOT NULL,
  due_date DATE NOT NULL,
  return_date DATE,
  FOREIGN KEY (book_id) REFERENCES Books(book_id) ON UPDATE CASCADE ON DELETE SET NULL,
  CONSTRAINT Chk_rental_date CHECK (return_date IS NULL OR return_date > rental_date) 
);