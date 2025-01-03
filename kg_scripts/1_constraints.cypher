CREATE CONSTRAINT node_constraint_movie_id IF NOT EXISTS FOR (n: Movie) REQUIRE n.movie_id IS UNIQUE;
CREATE CONSTRAINT node_constraint_collection_id IF NOT EXISTS FOR (n: Collection) REQUIRE n.collection_id IS UNIQUE;
CREATE CONSTRAINT node_constraint_language_id IF NOT EXISTS FOR (n: Language) REQUIRE n.language_id IS UNIQUE;
CREATE CONSTRAINT node_constraint_genre_id IF NOT EXISTS FOR (n: Genre) REQUIRE n.genre_id IS UNIQUE;
CREATE CONSTRAINT node_constraint_person_id IF NOT EXISTS FOR (n: Person) REQUIRE n.person_id IS UNIQUE;
CREATE CONSTRAINT node_constraint_country_id IF NOT EXISTS FOR (n: Country) REQUIRE n.country_id IS UNIQUE;
CREATE CONSTRAINT node_constraint_provider_id IF NOT EXISTS FOR (n: WatchProvider) REQUIRE n.provider_id IS UNIQUE;
CREATE CONSTRAINT node_constraint_company_id IF NOT EXISTS FOR (n: Company) REQUIRE n.company_id IS UNIQUE;

CREATE CONSTRAINT relationship_constraint_movie_collection IF NOT EXISTS FOR ()-[r: PART_OF]-() REQUIRE r.relationship_id IS UNIQUE;
CREATE CONSTRAINT relationship_constraint_movie_production IF NOT EXISTS FOR ()-[r: PRODUCED_BY]-() REQUIRE r.relationship_id IS UNIQUE;
CREATE CONSTRAINT relationship_constraint_movie_language IF NOT EXISTS FOR ()-[r: HAS_LANGUAGE]-() REQUIRE r.relationship_id IS UNIQUE;
CREATE CONSTRAINT relationship_constraint_movie_company IF NOT EXISTS FOR ()-[r: BASED_ON]-() REQUIRE r.relationship_id IS UNIQUE;
CREATE CONSTRAINT relationship_constraint_movie_genre IF NOT EXISTS FOR ()-[r: HAS_GENRE]-() REQUIRE r.relationship_id IS UNIQUE;
CREATE CONSTRAINT relationship_constraint_movie_provider IF NOT EXISTS FOR ()-[r: AVAILABLE_ON]-() REQUIRE r.relationship_id AND r.type IS UNIQUE;

CREATE CONSTRAINT relationship_constraint_movie_writter IF NOT EXISTS FOR ()-[r: WRITTEN_BY]-() REQUIRE r.relationship_id IS UNIQUE;
CREATE CONSTRAINT relationship_constraint_movie_editor IF NOT EXISTS FOR ()-[r: EDITED_BY]-() REQUIRE r.relationship_id IS UNIQUE;
CREATE CONSTRAINT relationship_constraint_movie_crew IF NOT EXISTS FOR ()-[r: CREW_BY]-() REQUIRE r.relationship_id IS UNIQUE;
CREATE CONSTRAINT relationship_constraint_movie_directed IF NOT EXISTS FOR ()-[r: DIRECTED_BY]-() REQUIRE r.relationship_id IS UNIQUE;
CREATE CONSTRAINT relationship_constraint_movie_camera IF NOT EXISTS FOR ()-[r: CAMERA_BY]-() REQUIRE r.relationship_id IS UNIQUE;
CREATE CONSTRAINT relationship_constraint_movie_lightning IF NOT EXISTS FOR ()-[r: LIGHTNING_BY]-() REQUIRE r.relationship_id IS UNIQUE;
CREATE CONSTRAINT relationship_constraint_movie_custome_makeup IF NOT EXISTS FOR ()-[r: COSTUMED_AND_MAKEUP_BY]-() REQUIRE r.relationship_id IS UNIQUE;
CREATE CONSTRAINT relationship_constraint_movie_sound IF NOT EXISTS FOR ()-[r: SOUND_BY]-() REQUIRE r.relationship_id IS UNIQUE;
CREATE CONSTRAINT relationship_constraint_movie_producer IF NOT EXISTS FOR ()-[r: PRODUCED_BY]-() REQUIRE r.relationship_id IS UNIQUE;
CREATE CONSTRAINT relationship_constraint_movie_art IF NOT EXISTS FOR ()-[r: ART_BY]-() REQUIRE r.relationship_id IS UNIQUE;
CREATE CONSTRAINT relationship_constraint_movie_vfx IF NOT EXISTS FOR ()-[r: VISUAL_EFFECTS_BY]-() REQUIRE r.relationship_id IS UNIQUE;
CREATE CONSTRAINT relationship_constraint_cast_movie IF NOT EXISTS FOR ()-[r: ACTED_IN]-() REQUIRE r.relationship_id IS UNIQUE;