GRANT SELECT ON northwind.* TO 'northwind_reader'@'localhost' IDENTIFIED BY 'reader';
GRANT ALL PRIVILEGES ON northwind.* TO 'northwind_admin'@'localhost' IDENTIFIED BY 'admin';