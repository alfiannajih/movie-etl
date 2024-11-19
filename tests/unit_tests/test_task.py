import unittest
import sys
import pathlib
import os
import json
from unittest.mock import patch, MagicMock

sys.path.append(str(pathlib.Path(os.path.dirname(os.path.realpath(__file__)), "src")))

from src.movie_etl.tasks.etl_task import (
    get_movie_ids,
    clean_movie_details,
    get_movie_ids,
    get_data_from_tmdb_api,
    clean_collection_details,
    clean_company_details,
    clean_person_details,
    clean_watch_providers,
    load_single_row_to_db,
    load_multi_row_to_db
)

class UnitTestETLTask(unittest.IsolatedAsyncioTestCase):
    @patch("src.movie_etl.tasks.etl_task.requests.get")
    async def test_get_movie_ids(self, mock_movie_discover):
        with open("./tests/unit_tests/mock_apis/discover_movie_page_1.json", "r") as fp:
            mock_response_page_1 = json.load(fp)

        with open("./tests/unit_tests/mock_apis/discover_movie_page_2.json", "r") as fp:
            mock_response_page_2 = json.load(fp)
        
        mock_movie_discover.return_value.json.side_effect = [mock_response_page_1, mock_response_page_2]

        movie_ids = await get_movie_ids.fn(
            start_date="2024-01-01",
            end_date="2024-01-07",
            url="https://api.themoviedb.org/3/discover/movie",
            vote_count_minimum=10,
            original_language="en"
        )

        expected_movie_ids = [
            1211957,
            1072342,
            1213997,
            1136347,
            1072876,
            972433,
            1223050,
            1214490,
            836972,
            1212662,
            912600,
            1154655,
            1212661,
            1203498,
            941883,
            1225332,
            1227163,
            1036156,
            1026468,
            1225686,
            1225425,
            1374463
        ]

        self.assertCountEqual(movie_ids, expected_movie_ids)
        self.assertListEqual(sorted(movie_ids), sorted(expected_movie_ids))

    async def test_clean_movie_details(self):
        with open("./tests/unit_tests/mock_apis/movie_details_912649.json", "r") as fp:
            mock_movie_details = json.load(fp)

        clean_details = await clean_movie_details.fn(
            movie_id=912649,
            movie_details=mock_movie_details
        )

        with open("./tests/unit_tests/expected_results/clean_movie_details_912649.json", "r") as fp:
            expected_clean_details = json.load(fp)

        self.assertEqual(clean_details["collection_id"], expected_clean_details["collection_id"])
        self.assertEqual(clean_details["movie_id"], expected_clean_details["movie_id"])
        self.assertEqual(clean_details["title"], expected_clean_details["title"])
        self.assertEqual(clean_details["overview"], expected_clean_details["overview"])
        self.assertEqual(clean_details["release_date"], expected_clean_details["release_date"])
        self.assertEqual(clean_details["popularity"], expected_clean_details["popularity"])
        self.assertEqual(clean_details["budget"], expected_clean_details["budget"])
        self.assertEqual(clean_details["revenue"], expected_clean_details["revenue"])
        self.assertEqual(clean_details["runtime"], expected_clean_details["runtime"])

        self.assertCountEqual(clean_details["production_countries"], expected_clean_details["production_countries"])
        self.assertCountEqual(clean_details["genres"], expected_clean_details["genres"])
        self.assertCountEqual(clean_details["production_companies"], expected_clean_details["production_companies"])
        self.assertCountEqual(clean_details["spoken_languages"], expected_clean_details["spoken_languages"])

        self.assertCountEqual(clean_details["casts"], expected_clean_details["casts"])
        self.assertCountEqual(clean_details["crews"], expected_clean_details["crews"])

    async def test_clean_collection_details(self):
        with open("./tests/unit_tests/mock_apis/collection_details_558216.json", "r") as fp:
            mock_collection_details = json.load(fp)

        clean_details = await clean_collection_details.fn(
            collection_id=912649,
            collection_details=mock_collection_details
        )

        with open("./tests/unit_tests/expected_results/clean_collection_details_558216.json", "r") as fp:
            expected_clean_details = json.load(fp)

        self.assertDictEqual(clean_details, expected_clean_details)

    async def test_clean_company_details(self):
        with open("./tests/unit_tests/mock_apis/company_details_5.json", "r") as fp:
            mock_company_details = json.load(fp)

        clean_details = await clean_company_details.fn(
            company_id=912649,
            company_details=mock_company_details
        )

        with open("./tests/unit_tests/expected_results/clean_company_details_5.json", "r") as fp:
            expected_clean_details = json.load(fp)

        self.assertDictEqual(clean_details, expected_clean_details)

    async def test_clean_person_details(self):
        with open("./tests/unit_tests/mock_apis/person_details_2524.json", "r") as fp:
            mock_person_details = json.load(fp)

        clean_details = await clean_person_details.fn(
            person_id=912649,
            person_details=mock_person_details
        )

        with open("./tests/unit_tests/expected_results/clean_person_details_2524.json", "r") as fp:
            expected_clean_details = json.load(fp)

        self.assertDictEqual(clean_details, expected_clean_details)
    
    async def test_clean_watch_providers(self):
        with open("./tests/unit_tests/mock_apis/watch_providers_123.json", "r") as fp:
            mock_watch_providers = json.load(fp)

        clean_providers = await clean_watch_providers.fn(
            movie_id=123,
            watch_providers=mock_watch_providers
        )

        with open("./tests/unit_tests/expected_results/clean_watch_providers_123.txt", "r") as fp:
            expected_clean_providers = [eval(line.strip()) for line in fp]
        
        self.assertCountEqual(clean_providers, expected_clean_providers)

    @patch("src.movie_etl.tasks.etl_task.Engine")
    @patch("src.movie_etl.tasks.etl_task.get_run_logger")
    async def test_load_single_row_to_db(self, mock_engine, mock_logger):
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_logger_error = MagicMock()

        mock_engine.raw_connection.return_value = mock_connection
        mock_connection.cursor.return_value.__enter__.return_value = mock_cursor
        mock_connection.cursor.return_value.__exit__.return_value = None
        mock_logger.error.return_value = mock_logger_error

        table_name = "movie_collections"
        id = 558216
        with open("./tests/unit_tests/expected_results/clean_collection_details_558216.json", "r") as fp:
            data = json.load(fp)

        await load_single_row_to_db.fn(
            table_name=table_name,
            primary_key_id=id,
            data=data,
            engine=mock_engine
        )

        expected_query = """INSERT INTO movie_collections (
            collection_id, name, overview
        ) VALUES (
            %(collection_id)s, %(name)s, %(overview)s
        )""".replace('\n', '').replace('  ', '').strip()
        
        mock_cursor.execute.assert_called_once()
        actual_query = mock_cursor.execute.call_args[0][0].replace('\n', '').replace('  ', '').strip()

        self.assertEqual(actual_query, expected_query)
        self.assertEqual(mock_cursor.execute.call_args[0][1], data)

        mock_connection.commit.assert_called_once()
        mock_connection.close.assert_called_once()

    @patch("src.movie_etl.tasks.etl_task.Engine")
    @patch("src.movie_etl.tasks.etl_task.get_run_logger")
    async def test_load_multi_row_to_db(self, mock_engine, mock_logger):
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_logger_error = MagicMock()

        mock_engine.raw_connection.return_value = mock_connection
        mock_connection.cursor.return_value.__enter__.return_value = mock_cursor
        mock_connection.cursor.return_value.__exit__.return_value = None
        mock_logger.error.return_value = mock_logger_error

        table_name = "movie_provider"
        id = 123
        columns = ["movie_id", "country_id", "provider_id", "type"]
        with open("./tests/unit_tests/expected_results/clean_watch_providers_123.txt", "r") as fp:
            data = [eval(line.strip()) for line in fp]

        await load_multi_row_to_db.fn(
            table_name=table_name,
            columns=columns,
            data=data,
            engine=mock_engine
        )

        expected_query = f"""INSERT INTO movie_provider
        (movie_id, country_id, provider_id, type)
        VALUES
        {str(data)[1:-1]}
        ON CONFLICT DO NOTHING""".replace('\n', '').replace('  ', '').strip()
        
        mock_cursor.execute.assert_called_once()
        actual_query = mock_cursor.execute.call_args[0][0].replace('\n', '').replace('  ', '').strip()

        self.assertEqual(actual_query, expected_query)
        
        mock_connection.commit.assert_called_once()
        mock_connection.close.assert_called_once()

    async def test_exception_load_single_row_to_db_(self):
        pass

    async def test_exception_load_multi_row_to_db_(self):
        pass

    async def test_get_data_from_tmdb_api(self):
        pass

if __name__ == '__main__':
    unittest.main()