-- Create tables
CREATE TABLE IF NOT EXISTS team (
    id INT AUTO_INCREMENT PRIMARY KEY,
    `Team` VARCHAR(255),
    `ScoreDescending` VARCHAR(50),
    `Overs` FLOAT,
    `RPO` FLOAT,
    `Lead` INT,
    `Inns` INT,
    `Result` VARCHAR(20),
    `Opposition` VARCHAR(255),
    `Ground` VARCHAR(255),
    `Start Date` DATETIME,
    `Declared` TINYINT(1),
    `Wickets` INT
);

CREATE TABLE IF NOT EXISTS batting (
    id INT AUTO_INCREMENT PRIMARY KEY,
    `Player` VARCHAR(255),
    `RunsDescending` INT,
    `BF` INT,
    `4s` INT,
    `6s` INT,
    `SR` FLOAT,
    `Inns` INT,
    `Opposition` VARCHAR(255),
    `Ground` VARCHAR(255),
    `Start Date` DATETIME,
    `Not Out` TINYINT(1),
    `Team` VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS bowling (
    id INT AUTO_INCREMENT PRIMARY KEY,
    `Player` VARCHAR(255),
    `Overs` FLOAT,
    `Mdns` INT,
    `Runs` INT,
    `WktsDescending` INT,
    `Econ` FLOAT,
    `Inns` INT,
    `Opposition` VARCHAR(255),
    `Ground` VARCHAR(255),
    `Start Date` DATETIME,
    `Team` VARCHAR(255)
);