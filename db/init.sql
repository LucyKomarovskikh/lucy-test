CREATE DATABASE vacancies CHARACTER SET utf8 COLLATE utf8_unicode_ci;
use vacancies;

CREATE TABLE vacancies_desc (
  id varchar(200),
  created date,
  name varchar(255),
  status varchar(20)
);
