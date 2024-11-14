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
    "title" VARCHAR(255),
    "overview" TEXT,
    "release_date" DATE,
    "popularity" FLOAT(2),
    "budget" FLOAT(2),
    "revenue" FLOAT(2),
    "runtime" INTEGER
);

CREATE TABLE "companies" (
    "company_id" BIGINT PRIMARY KEY,
    "parent_company_id" BIGINT,
    "country_id" CHAR(2),
    "name" VARCHAR(255),
    "description" TEXT,
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
    "name" VARCHAR(255),
    "gender" VARCHAR(15),
    "biography" TEXT,
    "place_of_birth" VARCHAR(255),
    "birthday" DATE,
    "deathday" DATE,
    "popularity" FLOAT(2)
);

CREATE TABLE "countries" (
    "country_id" CHAR(2) PRIMARY KEY,
    "country" VARCHAR(30)
);

CREATE TABLE "watch_providers" (
    "provider_id" INTEGER PRIMARY KEY,
    "name" VARCHAR(255)
);

CREATE TABLE "rotten_tomatoes_details" (
    "rotten_tomatoes_id" VARCHAR(255) PRIMARY KEY,
    "movie_id" BIGINT,
    "critic_score" INTEGER,
    "audience_score" INTEGER
);

CREATE TABLE "imdb_details" (
    "imdb_id" VARCHAR(30) PRIMARY KEY,
    "movie_id" BIGINT,
    "imdb_score" FLOAT(2)
);

CREATE TABLE "metacritic_details" (
    "metacritic_id" VARCHAR(255) PRIMARY KEY,
    "movie_id" BIGINT,
    "meta_score" INTEGER,
    "user_score" INTEGER
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

CREATE TABLE "movie_provider" (
    "movie_id" BIGINT,
    "country_id" CHAR(2),
    "provider_id" INTEGER,
    "type" VARCHAR(10)
);

CREATE TABLE "production_country" (
    "movie_id" BIGINT,
    "country_id" CHAR(2)
);

ALTER TABLE "movies" ADD CONSTRAINT "fk_movies_to_collections" FOREIGN KEY ("collection_id") REFERENCES "movie_collections" ("collection_id");

ALTER TABLE "companies" ADD CONSTRAINT "fk_parent_to_company" FOREIGN KEY ("parent_company_id") REFERENCES "companies" ("company_id");

ALTER TABLE "companies" ADD CONSTRAINT "fk_companies_to_countries" FOREIGN KEY ("country_id") REFERENCES "countries" ("country_id");

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

ALTER TABLE "movie_provider" ADD CONSTRAINT "fk_movie_provider_to_movies" FOREIGN KEY ("movie_id") REFERENCES "movies" ("movie_id");

ALTER TABLE "movie_provider" ADD CONSTRAINT "fk_movie_provider_to_countries" FOREIGN KEY ("country_id") REFERENCES "countries" ("country_id");

ALTER TABLE "movie_provider" ADD CONSTRAINT "fk_movie_provider_to_watch_providers" FOREIGN KEY ("provider_id") REFERENCES "watch_providers" ("provider_id");

ALTER TABLE "production_country" ADD CONSTRAINT "fk_production_country_to_movies" FOREIGN KEY ("movie_id") REFERENCES "movies" ("movie_id");

ALTER TABLE "production_country" ADD CONSTRAINT "fk_production_country_to_countries" FOREIGN KEY ("country_id") REFERENCES "countries" ("country_id");

ALTER TABLE "rotten_tomatoes_details" ADD CONSTRAINT "fk_rotten_tomatoes_details_to_movies" FOREIGN KEY ("movie_id") REFERENCES "movies" ("movie_id");

ALTER TABLE "imdb_details" ADD CONSTRAINT "fk_imdb_details_to_movies" FOREIGN KEY ("movie_id") REFERENCES "movies" ("movie_id");

ALTER TABLE "metacritic_details" ADD CONSTRAINT "fk_metacritic_details_to_movies" FOREIGN KEY ("movie_id") REFERENCES "movies" ("movie_id");

CREATE UNIQUE INDEX "movie_production_index" ON "movie_production" ("movie_id", "company_id");

CREATE UNIQUE INDEX "movie_language_index" ON "movie_language" ("movie_id", "language_id");

CREATE UNIQUE INDEX "movie_genre_index" ON "movie_genre" ("movie_id", "genre_id");

CREATE UNIQUE INDEX "movie_cast_index" ON "movie_cast" ("movie_id", "person_id", "character");

CREATE UNIQUE INDEX "movie_crew_index" ON "movie_crew" ("movie_id", "person_id", "job", "department");

CREATE UNIQUE INDEX "movie_provider_index" ON "movie_provider" ("movie_id", "country_id", "provider_id", "type");

CREATE UNIQUE INDEX "production_country_index" ON "production_country" ("movie_id", "country_id");