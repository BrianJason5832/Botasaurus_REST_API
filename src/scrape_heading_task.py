from botasaurus.request import request, Request
from sqlalchemy import create_engine, Column, Integer, String, Float, Boolean, JSON, TIMESTAMP, ForeignKey
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.dialects.postgresql import TEXT
from sqlalchemy.sql import text
import requests
import uuid
import json
import logging
import re

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Supabase database connection
db_url = 'postgresql://postgres.updemyybuhoicisjksiu:Dexter_#254@aws-0-eu-central-1.pooler.supabase.com:6543/postgres'
try:
    engine = create_engine(db_url)
    Session = sessionmaker(bind=engine)
    session = Session()
except Exception as e:
    logger.error(f"Failed to connect to Supabase: {str(e)}")
    exit(1)

# SQLAlchemy base
Base = declarative_base()

# Modify tasks table to use BIGINT for sort_id
try:
    with engine.connect() as connection:
        result = connection.execute(text("""
            SELECT data_type 
            FROM information_schema.columns 
            WHERE table_name = 'tasks' AND column_name = 'sort_id';
        """)).fetchone()
        
        if result and result[0].lower() != 'bigint':
            connection.execute(text("ALTER TABLE tasks ALTER COLUMN sort_id TYPE BIGINT;"))
            logger.info("Successfully altered tasks.sort_id to BIGINT")
        else:
            logger.info("tasks.sort_id is already BIGINT or table does not exist")
        
        # Check and rename scraper_name to google_maps_data_extractor
        result = connection.execute(text("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'tasks' AND column_name = 'scraper_name';
        """)).fetchone()
        
        if result:
            connection.execute(text("ALTER TABLE tasks RENAME COLUMN scraper_name TO google_maps_data_extractor;"))
            logger.info("Successfully renamed tasks.scraper_name to google_maps_data_extractor")
        else:
            logger.info("scraper_name column does not exist or already renamed")

        # Check and add task_name if missing
        result = connection.execute(text("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'tasks' AND column_name = 'task_name';
        """)).fetchone()
        
        if not result:
            connection.execute(text("ALTER TABLE tasks ADD COLUMN task_name TEXT;"))
            logger.info("Successfully added tasks.task_name")
        else:
            logger.info("task_name column already exists")

        # Check and add is_all_task if missing
        result = connection.execute(text("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'tasks' AND column_name = 'is_all_task';
        """)).fetchone()
        
        if not result:
            connection.execute(text("ALTER TABLE tasks ADD COLUMN is_all_task BOOLEAN DEFAULT FALSE;"))
            logger.info("Successfully added tasks.is_all_task")
        else:
            logger.info("is_all_task column already exists")

        # Check and add is_sync if missing
        result = connection.execute(text("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'tasks' AND column_name = 'is_sync';
        """)).fetchone()
        
        if not result:
            connection.execute(text("ALTER TABLE tasks ADD COLUMN is_sync BOOLEAN DEFAULT FALSE;"))
            logger.info("Successfully added tasks.is_sync")
        else:
            logger.info("is_sync column already exists")

        # Check and add parent_task_id if missing
        result = connection.execute(text("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'tasks' AND column_name = 'parent_task_id';
        """)).fetchone()
        
        if not result:
            connection.execute(text("ALTER TABLE tasks ADD COLUMN parent_task_id INTEGER REFERENCES tasks(id);"))
            logger.info("Successfully added tasks.parent_task_id")
        else:
            logger.info("parent_task_id column already exists")

        # Check and add meta_data if missing
        result = connection.execute(text("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'tasks' AND column_name = 'meta_data';
        """)).fetchone()
        
        if not result:
            connection.execute(text("ALTER TABLE tasks ADD COLUMN meta_data JSON;"))
            logger.info("Successfully added tasks.meta_data")
        else:
            logger.info("meta_data column already exists")

        # Check and add scraper_type if missing
        result = connection.execute(text("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'tasks' AND column_name = 'scraper_type';
        """)).fetchone()
        
        if not result:
            connection.execute(text("ALTER TABLE tasks ADD COLUMN scraper_type TEXT;"))
            logger.info("Successfully added tasks.scraper_type")
        else:
            logger.info("scraper_type column already exists")

        # Check and add scraper_name if missing
        result = connection.execute(text("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'tasks' AND column_name = 'scraper_name';
        """)).fetchone()

        if not result:
            # Check if google_maps_data_extractor exists and rename it back
            result_gm = connection.execute(text("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = 'tasks' AND column_name = 'google_maps_data_extractor';
            """)).fetchone()
            if result_gm:
                connection.execute(text("ALTER TABLE tasks RENAME COLUMN google_maps_data_extractor TO scraper_name;"))
                logger.info("Successfully renamed tasks.google_maps_data_extractor to scraper_name")
            else:
                connection.execute(text("ALTER TABLE tasks ADD COLUMN scraper_name TEXT;"))
                logger.info("Successfully added tasks.scraper_name")
        else:
            logger.info("scraper_name column already exists")
            
        connection.commit()
except Exception as e:
    logger.error(f"Error modifying tasks table: {str(e)}")

# Define models
class Place(Base):
    __tablename__ = 'places'
    place_id = Column(TEXT, primary_key=True)
    name = Column(TEXT)
    description = Column(TEXT)
    website = Column(TEXT)
    phone = Column(TEXT)
    is_spending_on_ads = Column(Boolean)
    rating = Column(Float)
    reviews_count = Column(Integer)
    main_category = Column(TEXT)
    workday_timing = Column(TEXT)
    is_temporarily_closed = Column(Boolean)
    is_permanently_closed = Column(Boolean)
    address = Column(TEXT)
    plus_code = Column(TEXT)
    link = Column(TEXT)
    status = Column(TEXT)
    price_range = Column(TEXT)
    reviews_link = Column(TEXT)
    time_zone = Column(TEXT)
    latitude = Column(Float)
    longitude = Column(Float)
    cid = Column(TEXT)
    data_id = Column(TEXT)

class RawPlaceData(Base):
    __tablename__ = 'raw_place_data'
    place_id = Column(TEXT, ForeignKey('places.place_id'), primary_key=True)
    raw_data = Column(JSON)

class Owner(Base):
    __tablename__ = 'owners'
    gmaps_id = Column(TEXT, primary_key=True)
    name = Column(TEXT)
    link = Column(TEXT)

class PlaceOwner(Base):
    __tablename__ = 'place_owners'
    place_id = Column(TEXT, ForeignKey('places.place_id'), primary_key=True)
    owner_id = Column(TEXT, ForeignKey('owners.gmaps_id'), primary_key=True)

class Category(Base):
    __tablename__ = 'categories'
    id = Column(Integer, primary_key=True)
    name = Column(TEXT, unique=True)

class PlaceCategory(Base):
    __tablename__ = 'place_categories'
    place_id = Column(TEXT, ForeignKey('places.place_id'), primary_key=True)
    category_id = Column(Integer, ForeignKey('categories.id'), primary_key=True)

class Hour(Base):
    __tablename__ = 'hours'
    id = Column(Integer, primary_key=True)
    place_id = Column(TEXT, ForeignKey('places.place_id'))
    day = Column(TEXT)
    open_time = Column(TEXT)
    close_time = Column(TEXT)

class DetailedAddress(Base):
    __tablename__ = 'detailed_address'
    place_id = Column(TEXT, ForeignKey('places.place_id'), primary_key=True)
    street = Column(TEXT)
    city = Column(TEXT)
    state = Column(TEXT)
    postal_code = Column(TEXT)
    country_code = Column(TEXT)

class Review(Base):
    __tablename__ = 'reviews'
    review_id = Column(TEXT, primary_key=True)
    place_id = Column(TEXT, ForeignKey('places.place_id'))
    rating = Column(Integer)
    name = Column(TEXT)
    reviewer_profile = Column(TEXT)
    review_text = Column(TEXT)
    published_at = Column(TIMESTAMP)
    response_from_owner_text = Column(TEXT)
    response_from_owner_date = Column(TIMESTAMP)

class FeaturedQuestion(Base):
    __tablename__ = 'featured_questions'
    place_id = Column(TEXT, ForeignKey('places.place_id'), primary_key=True)
    question = Column(TEXT)
    answer = Column(TEXT)
    question_date = Column(TIMESTAMP)
    answer_date = Column(TIMESTAMP)
    asked_by_name = Column(TEXT)
    answered_by_name = Column(TEXT)
    answered_by_link = Column(TEXT)

class ReviewKeyword(Base):
    __tablename__ = 'review_keywords'
    id = Column(Integer, primary_key=True)
    place_id = Column(TEXT, ForeignKey('places.place_id'))
    keyword = Column(TEXT)
    count = Column(Integer)

class Image(Base):
    __tablename__ = 'images'
    id = Column(Integer, primary_key=True)
    place_id = Column(TEXT, ForeignKey('places.place_id'))
    about = Column(TEXT)
    link = Column(TEXT)

class About(Base):
    __tablename__ = 'about'
    id = Column(Integer, primary_key=True)
    place_id = Column(TEXT, ForeignKey('places.place_id'))
    section_id = Column(TEXT)
    section_name = Column(TEXT)

class AboutOption(Base):
    __tablename__ = 'about_options'
    id = Column(Integer, primary_key=True)
    about_id = Column(Integer, ForeignKey('about.id'))
    name = Column(TEXT)
    enabled = Column(Boolean)

# Create custom tables
Base.metadata.create_all(engine)

@request
def scrape_heading_task(request: Request, data):
    """
    Botasaurus task to scrape Google Maps and store results in Supabase.
    
    Args:
        request (Request): Botasaurus Request object
        data (dict): Input data from UI controls
            - queries: List of search queries
            - api_key: API token
            - enable_reviews_extraction: Boolean
            - max_reviews: Max reviews per place
            - reviews_sort: Reviews sort option
            - lang: Language code
            - max_results: Max results per query
            - coordinates: Latitude,longitude
            - zoom_level: Zoom level (1-21)
    
    Returns:
        dict: Summary of processed queries and stored places with full JSON data
    """
    logger.info("Starting scrape_heading_task with input: %s", json.dumps(data, indent=2))
    
    # Extract input data
    queries = data.get("queries", [])
    api_key = data.get("api_key", None)
    enable_reviews_extraction = data.get("enable_reviews_extraction", False)
    max_reviews = data.get("max_reviews", None)
    reviews_sort = data.get("reviews_sort", "newest")
    lang = data.get("lang", "en")
    max_results = data.get("max_results", None)
    coordinates = data.get("coordinates", "42.6526,-73.7562")  # Default for Albany, NY
    zoom_level = data.get("zoom_level", 11)

    # Validate inputs
    if not queries:
        logger.error("No search queries provided")
        return {"error": "No search queries provided", "status": "failed"}
    if not api_key:
        logger.error("API key is required")
        return {"error": "API key is required", "status": "failed"}

    # Format location (handle coordinates starting with @)
    try:
        coords = coordinates.strip().lstrip('@')
        location = f"@{coords},{zoom_level}z"
    except Exception as e:
        logger.error(f"Invalid coordinates or zoom level: {str(e)}")
        return {"error": f"Invalid coordinates or zoom level: {str(e)}", "status": "failed"}

    def search_google_maps(query, page=1, location=location, language=lang, region="us", extra=True, api_token=api_key):
        """
        Search Google Maps using the GmapsExtractor API.
        """
        logger.info("Calling GmapsExtractor API for query: %s, page: %d", query, page)
        url = "https://cloud.gmapsextractor.com/api/v2/search"
        
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_token}"
        }
        
        payload = {
            "q": query,
            "page": page,
            "ll": location,
            "hl": language,
            "gl": region,
            "extra": extra
        }
        
        try:
            response = requests.post(url, headers=headers, json=payload)
            response.raise_for_status()
            logger.info("API call successful for query: %s", query)
            return response.json()
        except requests.exceptions.HTTPError as http_err:
            logger.error(f"HTTP error occurred: {http_err}")
            return None
        except requests.exceptions.RequestException as err:
            logger.error(f"Error occurred: {err}")
            return None

    def print_search_results(results):
        """
        Print formatted search results for debugging.
        """
        if not results or "data" not in results:
            logger.warning("No results found or error in response")
            return
        
        logger.info(f"Total Results: {results['total']}")
        logger.info("\nSearch Parameters:")
        for key, value in results['params'].items():
            logger.info(f"  {key}: {value}")
        
        logger.info("\nResults:")
        for i, item in enumerate(results['data'], 1):
            logger.info(f"\n{i}. {item['name']}")
            logger.info(f"   Categories: {item.get('categories', 'N/A')}")
            logger.info(f"   Address: {item.get('full_address', 'N/A')}")
            logger.info(f"   Website: {item.get('website', 'N/A')}")
            logger.info(f"   Rating: {item.get('average_rating', 'N/A')} ({item.get('review_count', '0')} reviews)")
            logger.info(f"   Email: {', '.join(item.get('emails', [])) or 'N/A'}")
            logger.info(f"   Phone: {item.get('phone', 'N/A')}")
            logger.info(f"   Opening Hours: {item.get('opening_hours', 'N/A')}")

    # Process queries
    all_results = []
    for query in queries:
        logger.info(f"Processing query: {query}")
        
        # Call the API
        result = search_google_maps(query=query)
        
        if not result or "data" not in result:
            logger.error(f"No results found or API error for query '{query}'")
            all_results.append({
                "query": query,
                "error": "No results found or API error",
                "status": "failed"
            })
            continue

        # Filter results
        places = result.get("data", [])
        if max_results:
            places = places[:max_results]

        # Store places in database and collect full JSON data
        query_results = {
            "query": query,
            "total": result.get("total", 0),
            "places": [],
            "status": "running"
        }

        for place in places:
            place_id = place.get("place_id")
            lat = place.get("latitude")
            lon = place.get("longitude")
            is_spending_on_ads = place.get("tracking_ids", {}).get("google", {}).get("ads") is not None

            # Store place
            place_record = Place(
                place_id=place_id,
                name=place.get("name"),
                description=place.get("meta", {}).get("description"),
                website=place.get("website"),
                phone=place.get("phone"),
                is_spending_on_ads=is_spending_on_ads,
                rating=place.get("average_rating"),
                reviews_count=place.get("review_count"),
                main_category=place.get("categories"),
                workday_timing=place.get("opening_hours"),
                is_temporarily_closed=place.get("is_temporarily_closed", False),
                is_permanently_closed=place.get("is_permanently_closed", False),
                address=place.get("full_address"),
                plus_code=place.get("plus_code"),
                link=place.get("google_maps_url"),
                status=place.get("status"),
                price_range=place.get("price_range"),
                reviews_link=place.get("review_url"),
                time_zone=place.get("time_zone"),
                latitude=lat,
                longitude=lon,
                cid=place.get("cid"),
                data_id=place.get("data_id")
            )
            session.add(place_record)

            # Store raw JSON data
            raw_place_data = RawPlaceData(
                place_id=place_id,
                raw_data=place
            )
            session.add(raw_place_data)

            # Flush Place to ensure it's in the database
            try:
                session.flush()
                logger.info(f"Flushed place {place_id}")
            except Exception as e:
                session.rollback()
                logger.error(f"Error flushing place {place_id}: {str(e)}")
                continue

            # Store detailed address
            addr_parts = place.get("full_address", "").split(", ")
            detailed_address = DetailedAddress(
                place_id=place_id,
                street=addr_parts[0] if len(addr_parts) > 0 else None,
                city=addr_parts[1] if len(addr_parts) > 1 else None,
                state=addr_parts[2] if len(addr_parts) > 2 else None,
                postal_code=addr_parts[3] if len(addr_parts) > 3 else None,
                country_code=addr_parts[4] if len(addr_parts) > 4 else None
            )
            session.add(detailed_address)

            # Store categories (disable autoflush)
            categories = place.get("categories", "").split(", ") if place.get("categories") else []
            with session.no_autoflush:
                for cat_name in categories:
                    if cat_name:
                        category = session.query(Category).filter_by(name=cat_name).first()
                        if not category:
                            category = Category(name=cat_name)
                            session.add(category)
                            session.flush()
                            logger.info(f"Added new category: {cat_name}")
                        place_category = PlaceCategory(
                            place_id=place_id,
                            category_id=category.id
                        )
                        session.add(place_category)

            # Store reviews metadata (placeholder)
            if enable_reviews_extraction:
                review_id = str(uuid.uuid4())
                reviews_info = {
                    "enabled": enable_reviews_extraction,
                    "max_reviews": max_reviews,
                    "sort": reviews_sort,
                    "note": "Reviews extraction not supported by this API"
                }
                review = Review(
                    review_id=review_id,
                    place_id=place_id,
                    rating=None,
                    name=None,
                    reviewer_profile=None,
                    review_text=json.dumps(reviews_info),
                    published_at=None,
                    response_from_owner_text=None,
                    response_from_owner_date=None
                )
                session.add(review)

            # Store about (categories metadata)
            if categories:
                about_record = About(
                    place_id=place_id,
                    section_id=str(uuid.uuid4()),
                    section_name="Categories"
                )
                session.add(about_record)

            # Store images
            featured_image = place.get("featured_image")
            if featured_image:
                image_record = Image(
                    place_id=place_id,
                    about=place.get("categories"),  # Store categories as about
                    link=featured_image
                )
                session.add(image_record)

            # Store reviews (place-level data as a single review)
            if place.get("average_rating") or place.get("name") or place.get("review_url"):
                review_id = str(uuid.uuid4())
                review_record = Review(
                    review_id=review_id,
                    place_id=place_id,
                    rating=int(place.get("average_rating", 0)) if place.get("average_rating") else None,
                    name=place.get("name"),
                    reviewer_profile=place.get("review_url"),
                    review_text=None,
                    published_at=None,
                    response_from_owner_text=None,
                    response_from_owner_date=None
                )
                session.add(review_record)

            # Parse and store hours
            opening_hours = place.get("opening_hours")
            if opening_hours:
                # Example: "Monday: [7:30 AM-3:30 PM], Tuesday: [7:30 AM-3:30 PM], ..., Sunday: [Closed]"
                hours_entries = re.findall(r"(\w+):\s*\[(.*?)\]", opening_hours)
                for day, hours in hours_entries:
                    if hours.lower() == "closed":
                        hour_record = Hour(
                            place_id=place_id,
                            day=day,
                            open_time=None,
                            close_time="Closed"
                        )
                    else:
                        # Extract open and close times (e.g., "7:30 AM-3:30 PM")
                        times = hours.split("-")
                        if len(times) == 2:
                            open_time, close_time = times
                            hour_record = Hour(
                                place_id=place_id,
                                day=day,
                                open_time=open_time.strip(),
                                close_time=close_time.strip()
                            )
                        else:
                            continue  # Skip malformed hours
                    session.add(hour_record)

            # Add full JSON to query results
            query_results["places"].append(place)

        # Commit transaction
        try:
            session.commit()
            logger.info(f"Successfully stored {len(places)} places for query '{query}'")
            query_results["places_stored"] = len(places)
            query_results["status"] = "completed"
        except Exception as e:
            session.rollback()
            logger.error(f"Error storing places for query '{query}': {str(e)}")
            query_results["error"] = f"Database error: {str(e)}"
            query_results["status"] = "failed"

        all_results.append(query_results)

    # Query and print stored places
    logger.info("Stored Places in Database:")
    stored_places = session.query(Place).all()
    for place in stored_places:
        logger.info(f"ID: {place.place_id}, Name: {place.name}, Address: {place.address}, Rating: {place.rating}")

    final_result = {
        "results": all_results,
        "status": "completed"
    }
    logger.info("Task completed with result: %s", json.dumps(final_result, indent=2))
    return final_result