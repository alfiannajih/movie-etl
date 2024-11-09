DROP SCHEMA public CASCADE;
CREATE SCHEMA public;

CREATE TABLE "movie_collections" (
    "collection_id" BIGINT PRIMARY KEY,
    "name" VARCHAR(255),
    "overview" TEXT
);

CREATE TABLE "movies" (
    "movie_id" BIGINT PRIMARY KEY,
    "collection_id" BIGINT,
    "imdb_id" VARCHAR(20),
    "title" VARCHAR(255),
    "overview" TEXT,
    "release_date" DATE,
    "popularity" FLOAT(2),
    "vote_average" FLOAT(2),
    "vote_count" INTEGER,
    "budget" FLOAT(2),
    "revenue" FLOAT(2),
    "runtime" INTEGER
);

CREATE TABLE "companies" (
    "company_id" BIGINT PRIMARY KEY,
    "parent_company_id" BIGINT,
    "name" VARCHAR(255),
    "description" TEXT,
    "country" VARCHAR(255),
    "head_quarters" VARCHAR(255)
);

CREATE TABLE "languages" (
    "language_id" CHAR(2) PRIMARY KEY,
    "language" VARCHAR(30)
);

CREATE TABLE "genres" (
    "genre_id" INTEGER PRIMARY KEY,
    "genre" VARCHAR(30)
);

CREATE TABLE "people" (
    "person_id" BIGINT PRIMARY KEY,
    "imdb_id" VARCHAR(20),
    "name" VARCHAR(255),
    "gender" VARCHAR(20),
    "biography" TEXT,
    "place_of_birth" VARCHAR(255),
    "birthday" DATE,
    "deathday" DATE,
    "popularity" FLOAT(2)
);

CREATE TABLE "tv_series" (
    "series_id" BIGINT PRIMARY KEY,
    "title" VARCHAR(255),
    "overview" TEXT,
    "popularity" FLOAT(2),
    "vote_average" FLOAT(2),
    "vote_count" INTEGER
);

CREATE TABLE "tv_seasons" (
    "season_id" BIGINT PRIMARY KEY,
    "series_id" BIGINT,
    "season_number" INTEGER,
    "air_date" DATE,
    "overview" TEXT,
    "vote_average" FLOAT(2)
);

CREATE TABLE "tv_episodes" (
    "episode_id" BIGINT PRIMARY KEY,
    "season_id" BIGINT,
    "episode_number" INTEGER,
    "title" VARCHAR(255),
    "air_date" DATE,
    "overview" TEXT,
    "production_code" INTEGER,
    "runtime" INTEGER,
    "vote_average" FLOAT(2),
    "vote_count" INTEGER

);

CREATE TABLE "movie_production" (
    "movie_id" BIGINT,
    "company_id" BIGINT
);

CREATE TABLE "movie_language" (
    "movie_id" BIGINT,
    "language_id" CHAR(2)
);

CREATE TABLE "movie_genre" (
    "movie_id" BIGINT,
    "genre_id" INTEGER
);

CREATE TABLE "movie_cast" (
    "movie_id" BIGINT,
    "person_id" BIGINT,
    "character" VARCHAR(255)
);

CREATE TABLE "movie_crew" (
    "movie_id" BIGINT,
    "person_id" BIGINT,
    "job" VARCHAR(255),
    "department" VARCHAR(255)
);

CREATE TABLE "series_production" (
    "series_id" BIGINT,
    "company_id" BIGINT
);

CREATE TABLE "series_language" (
    "series_id" BIGINT,
    "language_id" CHAR(2)
);

CREATE TABLE "series_genre" (
    "series_id" BIGINT,
    "genre_id" INTEGER
);

CREATE TABLE "episode_cast" (
    "episode_id" BIGINT,
    "person_id" BIGINT,
    "character" VARCHAR(255)
);

CREATE TABLE "episode_crew" (
    "episode_id" BIGINT,
    "person_id" BIGINT,
    "job" VARCHAR(255),
    "department" VARCHAR(255)
);

ALTER TABLE "movies" ADD CONSTRAINT "fk_movies_to_collections" FOREIGN KEY ("collection_id") REFERENCES "movie_collections" ("collection_id");

ALTER TABLE "companies" ADD CONSTRAINT "fk_parent_to_company" FOREIGN KEY ("parent_company_id") REFERENCES "companies" ("company_id");

ALTER TABLE "movie_production" ADD CONSTRAINT "fk_production_to_movies" FOREIGN KEY ("movie_id") REFERENCES "movies" ("movie_id");

ALTER TABLE "movie_production" ADD CONSTRAINT "fk_production_to_companies" FOREIGN KEY ("company_id") REFERENCES "companies" ("company_id");

ALTER TABLE "movie_language" ADD CONSTRAINT "fk_movie_language_to_movies" FOREIGN KEY ("movie_id") REFERENCES "movies" ("movie_id");

ALTER TABLE "movie_language" ADD CONSTRAINT "fk_movie_language_to_languages" FOREIGN KEY ("language_id") REFERENCES "languages" ("language_id");

ALTER TABLE "movie_genre" ADD CONSTRAINT "fk_movie_genre_to_movies" FOREIGN KEY ("movie_id") REFERENCES "movies" ("movie_id");

ALTER TABLE "movie_genre" ADD CONSTRAINT "fk_movie_genre_to_genres" FOREIGN KEY ("genre_id") REFERENCES "genres" ("genre_id");

ALTER TABLE "movie_cast" ADD CONSTRAINT "fk_movie_cast_to_movies" FOREIGN KEY ("movie_id") REFERENCES "movies" ("movie_id");

ALTER TABLE "movie_cast" ADD CONSTRAINT "fk_movie_cast_to_people" FOREIGN KEY ("person_id") REFERENCES "people" ("person_id");

ALTER TABLE "movie_crew" ADD CONSTRAINT "fk_movie_crew_to_movies" FOREIGN KEY ("movie_id") REFERENCES "movies" ("movie_id");

ALTER TABLE "movie_crew" ADD CONSTRAINT "fk_movie_crew_to_people" FOREIGN KEY ("person_id") REFERENCES "people" ("person_id");

ALTER TABLE "series_production" ADD CONSTRAINT "fk_series_production_to_tv_series" FOREIGN KEY ("series_id") REFERENCES "tv_series" ("series_id");

ALTER TABLE "series_production" ADD CONSTRAINT "fk_series_production_to_companies" FOREIGN KEY ("company_id") REFERENCES "companies" ("company_id");

ALTER TABLE "series_language" ADD CONSTRAINT "fk_series_language_to_tv_series" FOREIGN KEY ("series_id") REFERENCES "tv_series" ("series_id");

ALTER TABLE "series_language" ADD CONSTRAINT "fk_series_language_to_languages" FOREIGN KEY ("language_id") REFERENCES "languages" ("language_id");

ALTER TABLE "series_genre" ADD CONSTRAINT "fk_series_genre_to_tv_series" FOREIGN KEY ("series_id") REFERENCES "tv_series" ("series_id");

ALTER TABLE "series_genre" ADD CONSTRAINT "fk_series_genre_to_genres" FOREIGN KEY ("genre_id") REFERENCES "genres" ("genre_id");

ALTER TABLE "tv_seasons" ADD CONSTRAINT "fk_tv_seasons_to_tv_series" FOREIGN KEY ("series_id") REFERENCES "tv_series" ("series_id");

ALTER TABLE "tv_episodes" ADD CONSTRAINT "fk_tv_episodes_to_tv_seasons" FOREIGN KEY ("season_id") REFERENCES "tv_seasons" ("season_id");

ALTER TABLE "episode_cast" ADD CONSTRAINT "fk_episode_cast_to_tv_episodes" FOREIGN KEY ("episode_id") REFERENCES "tv_episodes" ("episode_id");

ALTER TABLE "episode_cast" ADD CONSTRAINT "fk_episode_cast_to_people" FOREIGN KEY ("person_id") REFERENCES "people" ("person_id");

ALTER TABLE "episode_crew" ADD CONSTRAINT "fk_episode_crew_to_tv_episodes" FOREIGN KEY ("episode_id") REFERENCES "tv_episodes" ("episode_id");

ALTER TABLE "episode_crew" ADD CONSTRAINT "fk_episode_crew_to_people" FOREIGN KEY ("person_id") REFERENCES "people" ("person_id");

CREATE UNIQUE INDEX "movie_production_index" ON movie_production ("movie_id", "company_id");

CREATE UNIQUE INDEX "movie_language_index" ON movie_language ("movie_id", "language_id");

CREATE UNIQUE INDEX "movie_genre_index" ON movie_genre ("movie_id", "genre_id");

CREATE UNIQUE INDEX "movie_cast_index" ON movie_cast ("movie_id", "person_id", "character");

CREATE UNIQUE INDEX "movie_crew_index" ON movie_crew ("movie_id", "person_id", "job", "department");

CREATE UNIQUE INDEX "series_production_index" ON series_production ("series_id", "company_id");

CREATE UNIQUE INDEX "series_language_index" ON series_language ("series_id", "language_id");

CREATE UNIQUE INDEX "series_genre_index" ON series_genre ("series_id", "genre_id");

CREATE UNIQUE INDEX "episode_cast_index" ON episode_cast ("episode_id", "person_id", "character");

CREATE UNIQUE INDEX "episode_crew_index" ON episode_crew ("episode_id", "person_id", "job", "department");

INSERT INTO "languages"
("language_id", "language")
VALUES
('kw', 'Cornish'),
('ff', 'Fulah'),
('gn', 'Guarani'),
('id', 'Indonesian'),
('lu', 'Luba-Katanga'),
('nr', 'Ndebele'),
('os', 'Ossetian; Ossetic'),
('ru', 'Russian'),
('se', 'Northern Sami'),
('so', 'Somali'),
('es', 'Spanish'),
('sv', 'Swedish'),
('ta', 'Tamil'),
('te', 'Telugu'),
('tn', 'Tswana'),
('uk', 'Ukrainian'),
('uz', 'Uzbek'),
('el', 'Greek'),
('co', 'Corsican'),
('dv', 'Divehi'),
('kk', 'Kazakh'),
('ki', 'Kikuyu'),
('or', 'Oriya'),
('si', 'Sinhalese'),
('st', 'Sotho'),
('sr', 'Serbian'),
('ss', 'Swati'),
('tr', 'Turkish'),
('wa', 'Walloon'),
('cn', 'Cantonese'),
('bi', 'Bislama'),
('cr', 'Cree'),
('cy', 'Welsh'),
('eu', 'Basque'),
('hz', 'Herero'),
('ho', 'Hiri Motu'),
('ka', 'Georgian'),
('kr', 'Kanuri'),
('km', 'Khmer'),
('kj', 'Kuanyama'),
('to', 'Tonga'),
('vi', 'Vietnamese'),
('zu', 'Zulu'),
('zh', 'Mandarin'),
('ps', 'Pushto'),
('mk', 'Macedonian'),
('ae', 'Avestan'),
('az', 'Azerbaijani'),
('ba', 'Bashkir'),
('sh', 'Serbo-Croatian'),
('lv', 'Latvian'),
('lt', 'Lithuanian'),
('ms', 'Malay'),
('rm', 'Raeto-Romance'),
('as', 'Assamese'),
('gd', 'Gaelic'),
('ja', 'Japanese'),
('ko', 'Korean'),
('ku', 'Kurdish'),
('mo', 'Moldavian'),
('mn', 'Mongolian'),
('nb', 'Norwegian Bokmål'),
('om', 'Oromo'),
('pi', 'Pali'),
('sq', 'Albanian'),
('vo', 'Volapük'),
('bo', 'Tibetan'),
('da', 'Danish'),
('kl', 'Kalaallisut'),
('kn', 'Kannada'),
('nl', 'Dutch'),
('nn', 'Norwegian Nynorsk'),
('sa', 'Sanskrit'),
('am', 'Amharic'),
('hy', 'Armenian'),
('bs', 'Bosnian'),
('hr', 'Croatian'),
('mh', 'Marshall'),
('mg', 'Malagasy'),
('ne', 'Nepali'),
('su', 'Sundanese'),
('ts', 'Tsonga'),
('ug', 'Uighur'),
('cs', 'Czech'),
('jv', 'Javanese'),
('ro', 'Romanian'),
('sm', 'Samoan'),
('tg', 'Tajik'),
('wo', 'Wolof'),
('br', 'Breton'),
('fr', 'French'),
('ga', 'Irish'),
('ht', 'Haitian; Haitian Creole'),
('kv', 'Komi'),
('mi', 'Maori'),
('th', 'Thai'),
('xx', 'No Language'),
('af', 'Afrikaans'),
('av', 'Avaric'),
('bm', 'Bambara'),
('ca', 'Catalan'),
('ce', 'Chechen'),
('de', 'German'),
('gv', 'Manx'),
('rw', 'Kinyarwanda'),
('ky', 'Kirghiz'),
('ln', 'Lingala'),
('sn', 'Shona'),
('yi', 'Yiddish'),
('be', 'Belarusian'),
('cu', 'Slavic'),
('dz', 'Dzongkha'),
('eo', 'Esperanto'),
('fi', 'Finnish'),
('fy', 'Frisian'),
('ie', 'Interlingue'),
('ia', 'Interlingua'),
('it', 'Italian'),
('ng', 'Ndonga'),
('pa', 'Punjabi'),
('pt', 'Portuguese'),
('rn', 'Rundi'),
('fa', 'Persian'),
('ch', 'Chamorro'),
('cv', 'Chuvash'),
('en', 'English'),
('hu', 'Hungarian'),
('ii', 'Yi'),
('kg', 'Kongo'),
('li', 'Limburgish'),
('ml', 'Malayalam'),
('nv', 'Navajo'),
('ny', 'Chichewa; Nyanja'),
('sg', 'Sango'),
('tw', 'Twi'),
('ab', 'Abkhazian'),
('ar', 'Arabic'),
('ee', 'Ewe'),
('fo', 'Faroese'),
('ik', 'Inupiaq'),
('ks', 'Kashmiri'),
('lb', 'Letzeburgesch'),
('nd', 'Ndebele'),
('oc', 'Occitan'),
('sk', 'Slovak'),
('tt', 'Tatar'),
('ve', 'Venda'),
('ay', 'Aymara'),
('fj', 'Fijian'),
('gu', 'Gujarati'),
('io', 'Ido'),
('lo', 'Lao'),
('la', 'Latin'),
('no', 'Norwegian'),
('oj', 'Ojibwa'),
('pl', 'Polish'),
('qu', 'Quechua'),
('sl', 'Slovenian'),
('sc', 'Sardinian'),
('sw', 'Swahili'),
('tl', 'Tagalog'),
('ur', 'Urdu'),
('bg', 'Bulgarian'),
('hi', 'Hindi'),
('yo', 'Yoruba'),
('ak', 'Akan'),
('an', 'Aragonese'),
('bn', 'Bengali'),
('et', 'Estonian'),
('gl', 'Galician'),
('ha', 'Hausa'),
('ig', 'Igbo'),
('iu', 'Inuktitut'),
('lg', 'Ganda'),
('mr', 'Marathi'),
('mt', 'Maltese'),
('my', 'Burmese'),
('na', 'Nauru'),
('sd', 'Sindhi'),
('xh', 'Xhosa'),
('za', 'Zhuang'),
('aa', 'Afar'),
('is', 'Icelandic'),
('ty', 'Tahitian'),
('ti', 'Tigrinya'),
('tk', 'Turkmen'),
('he', 'Hebrew')