import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone
from collections import Counter
import io
import os
import re
import time
import streamlit.components.v1 as components
from sqlalchemy import create_engine, text
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
import hashlib
def stable_hash(*values) -> str:
    s = "||".join("" if v is None else str(v) for v in values)
    return hashlib.md5(s.encode()).hexdigest()[:8]

st.set_page_config(page_title="Book Production Time Tracking", page_icon="favicon.png")

components.html(
    """
    <style>
      /* Kill default margins inside the iframe so no extra space sneaks in */
      html, body { margin: 0; padding: 0; height: 10px; overflow: hidden; }
    </style>
    <script>
    (function() {
        const doc = window.parent.document;
        const head = doc.head;

        // Load Google Fonts
        const link1 = doc.createElement('link');
        link1.rel = 'preconnect';
        link1.href = 'https://fonts.googleapis.com';
        head.appendChild(link1);

        const link2 = doc.createElement('link');
        link2.rel = 'preconnect';
        link2.href = 'https://fonts.gstatic.com';
        link2.crossOrigin = 'anonymous';
        head.appendChild(link2);

        const link3 = doc.createElement('link');
        link3.rel = 'stylesheet';
        link3.href = 'https://fonts.googleapis.com/css2?family=Source+Sans+3:ital,wght@0,200..900;1,200..900&display=swap';
        head.appendChild(link3);

        // Allow sidebar to be resizable
        const style = doc.createElement('style');
        style.textContent = '[data-testid="stSidebar"] { resize: horizontal; overflow: auto; }';
        head.appendChild(style);

        // Set default sidebar width so users can still resize it
        const sidebar = doc.querySelector('section[data-testid="stSidebar"]');
        if (sidebar) {
            if (window.innerWidth <= 768) {
                sidebar.style.width = '100%';
            } else {
                sidebar.style.width = '45%';
            }
        }

        // Clamp THIS component's iframe height to 10px
        const me = window.frameElement;
        if (me) {
            me.style.height = '10px';
            me.style.maxHeight = '10px';
            me.style.minHeight = '0';
            me.style.overflow = 'hidden';
        }
    })();
    </script>
    """,
    height=10,
)

# Set BST timezone (UTC+1)
BST = timezone(timedelta(hours=1))
UTC_PLUS_1 = BST  # Keep backward compatibility

# Error logging: capture all messages passed to st.error
if "error_log" not in st.session_state:
    st.session_state.error_log = []

_original_st_error = st.error

# Placeholder messages shown to users but not helpful in the error log
PLACEHOLDER_ERRORS = {
    "An unexpected error occurred, please see the error log for more details",
    "Database error, please see the error log for more details",
}


def log_error(message, *args, **kwargs):
    """Log error messages with timestamp and display them."""
    timestamp = datetime.now(BST).strftime("%Y-%m-%d %H:%M:%S")
    if message not in PLACEHOLDER_ERRORS:
        st.session_state.error_log.append({"time": timestamp, "message": message})
    _original_st_error(message, *args, **kwargs)


st.error = log_error

# Known full user names for matching CSV imports
EDITORIAL_USERS_LIST = [
    "Bethany Latham",
    "Charis Mather",
    "Noah Leatherland",
]
DESIGN_USERS_LIST = [
    "Amelia Harris",
    "Amy Li",
    "Drue Rintoul",
    "Jasmine Pointer",
    "Ker Ker Lee",
    "Rob Delph",
]
ALL_USERS_LIST = EDITORIAL_USERS_LIST + DESIGN_USERS_LIST

# Map first names (and common short forms) to full user names
FIRST_NAME_TO_FULL = {name.split()[0].lower(): name for name in ALL_USERS_LIST}
FIRST_NAME_TO_FULL.update({
    "beth": "Bethany Latham",
    "ker ker": "Ker Ker Lee",
})

def normalize_user_name(name):
    """Return a canonical user name from various CSV formats."""
    if name is None:
        return "Not set"
    name = str(name).strip()
    if name == "" or name == "Not set":
        return "Not set"

    lower = name.lower()
    # Exact match to known users
    for full in ALL_USERS_LIST:
        if lower == full.lower():
            return full

    # Match by first name or short form
    first = lower.split()[0]
    if first in FIRST_NAME_TO_FULL:
        return FIRST_NAME_TO_FULL[first]

    return name


@st.cache_resource
def init_database():
    """Initialise database connection and create tables"""
    try:
        # Prefer Streamlit secrets but allow an env var fallback
        database_url = st.secrets.get("database", {}).get("url") or os.getenv("DATABASE_URL")
        if not database_url:
            st.error(
                "Database URL not configured. Set database.url in Streamlit secrets "
                "or the DATABASE_URL environment variable."
            )
            return None

        engine = create_engine(database_url)

        # Create table if it doesn't exist
        with engine.connect() as conn:
            conn.execute(
                text(
                    '''
                CREATE TABLE IF NOT EXISTS trello_time_tracking (
                    id SERIAL PRIMARY KEY,
                    card_name VARCHAR(500) NOT NULL,
                    user_name VARCHAR(255) NOT NULL,
                    list_name VARCHAR(255) NOT NULL,
                    time_spent_seconds INTEGER NOT NULL,
                    date_started DATE,
                    card_estimate_seconds INTEGER,
                    board_name VARCHAR(255),
                    labels TEXT,
                    completed BOOLEAN DEFAULT FALSE,
                    archived BOOLEAN DEFAULT FALSE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(card_name, user_name, list_name, date_started, time_spent_seconds)
                )
            '''
                )
            )
            # Add archived column to existing table if it doesn't exist
            conn.execute(
                text(
                    '''
                ALTER TABLE trello_time_tracking
                ADD COLUMN IF NOT EXISTS archived BOOLEAN DEFAULT FALSE
            '''
                )
            )

            # Add session_start_time column if it doesn't exist
            conn.execute(
                text(
                    '''
                ALTER TABLE trello_time_tracking
                ADD COLUMN IF NOT EXISTS session_start_time TIMESTAMP
            '''
                )
            )

            # Add tag column if it doesn't exist
            conn.execute(
                text(
                    '''
                ALTER TABLE trello_time_tracking
                ADD COLUMN IF NOT EXISTS tag VARCHAR(255)
            '''
                )
            )

            # Ensure other optional columns exist for older databases
            conn.execute(
                text(
                    '''
                ALTER TABLE trello_time_tracking
                ADD COLUMN IF NOT EXISTS card_estimate_seconds INTEGER
            '''
                )
            )
            conn.execute(
                text(
                    '''
                ALTER TABLE trello_time_tracking
                ADD COLUMN IF NOT EXISTS board_name VARCHAR(255)
            '''
                )
            )
            conn.execute(
                text(
                    '''
                ALTER TABLE trello_time_tracking
                ADD COLUMN IF NOT EXISTS labels TEXT
            '''
                )
            )
            conn.execute(
                text(
                    '''
                ALTER TABLE trello_time_tracking
                ADD COLUMN IF NOT EXISTS created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            '''
                )
            )
            conn.execute(
                text(
                    '''
                ALTER TABLE trello_time_tracking
                ADD COLUMN IF NOT EXISTS completed BOOLEAN DEFAULT FALSE
            '''
                )
            )

            # Create books table for storing book metadata
            conn.execute(
                text(
                    '''
                CREATE TABLE IF NOT EXISTS books (
                    card_name VARCHAR(500) PRIMARY KEY,
                    board_name VARCHAR(255),
                    tag VARCHAR(255),
                    archived BOOLEAN DEFAULT FALSE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            '''
                )
            )

            # Add optional columns to books table if they are missing
            conn.execute(
                text(
                    '''
                ALTER TABLE books
                ADD COLUMN IF NOT EXISTS board_name VARCHAR(255)
            '''
                )
            )
            conn.execute(
                text(
                    '''
                ALTER TABLE books
                ADD COLUMN IF NOT EXISTS tag VARCHAR(255)
            '''
                )
            )
            conn.execute(
                text(
                    '''
                ALTER TABLE books
                ADD COLUMN IF NOT EXISTS archived BOOLEAN DEFAULT FALSE
            '''
                )
            )
            conn.execute(
                text(
                    '''
                ALTER TABLE books
                ADD COLUMN IF NOT EXISTS created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            '''
                )
            )

            # Create active timers table for persistent timer storage
            conn.execute(
                text(
                    '''
                CREATE TABLE IF NOT EXISTS active_timers (
                    id SERIAL PRIMARY KEY,
                    timer_key VARCHAR(500) NOT NULL UNIQUE,
                    card_name VARCHAR(255) NOT NULL,
                    user_name VARCHAR(100),
                    list_name VARCHAR(100) NOT NULL,
                    board_name VARCHAR(100),
                    start_time TIMESTAMPTZ NOT NULL,
                    accumulated_seconds INTEGER DEFAULT 0,
                    is_paused BOOLEAN DEFAULT FALSE,
                    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
                )
            '''
                )
            )

            # Add new columns to existing active_timers table if they don't exist
            conn.execute(
                text(
                    '''
                ALTER TABLE active_timers
                ADD COLUMN IF NOT EXISTS accumulated_seconds INTEGER DEFAULT 0
            '''
                )
            )
            conn.execute(
                text(
                    '''
                ALTER TABLE active_timers
                ADD COLUMN IF NOT EXISTS is_paused BOOLEAN DEFAULT FALSE
            '''
                )
            )

            # Migrate existing TIMESTAMP columns to TIMESTAMPTZ if needed
            try:
                conn.execute(
                    text(
                        '''
                    ALTER TABLE active_timers
                    ALTER COLUMN start_time TYPE TIMESTAMPTZ USING start_time AT TIME ZONE 'Europe/London'
                '''
                    )
                )
                conn.execute(
                    text(
                        '''
                    ALTER TABLE active_timers
                    ALTER COLUMN created_at TYPE TIMESTAMPTZ USING created_at AT TIME ZONE 'Europe/London'
                '''
                    )
                )
            except Exception:
                # Columns might already be TIMESTAMPTZ, ignore the error
                pass
            conn.commit()

        return engine
    except Exception as e:
        st.error(f"Database initialisation failed: {str(e)}")
        return None


def get_users_from_database(_engine):
    """Get list of unique users from database with retry logic"""
    max_retries = 3
    for attempt in range(max_retries):
        try:
            with _engine.connect() as conn:
                result = conn.execute(
                    text(
                        'SELECT DISTINCT COALESCE(user_name, \'Not set\') FROM trello_time_tracking ORDER BY COALESCE(user_name, \'Not set\')'
                    )
                )
                return [row[0] for row in result]
        except Exception as e:
            if attempt < max_retries - 1:
                time.sleep(0.5)
                continue
            else:
                return []
    return []


def get_tags_from_database(_engine):
    """Get list of unique individual tags from database, splitting comma-separated values"""
    max_retries = 3
    for attempt in range(max_retries):
        try:
            with _engine.connect() as conn:
                result = conn.execute(
                    text(
                        "SELECT DISTINCT tag FROM trello_time_tracking WHERE tag IS NOT NULL AND tag != '' ORDER BY tag"
                    )
                )
                all_tag_strings = [row[0] for row in result]

                # Split comma-separated tags and create unique set
                individual_tags = set()
                for tag_string in all_tag_strings:
                    if tag_string:
                        # Split by comma and strip whitespace
                        tags_in_string = [tag.strip() for tag in tag_string.split(',')]
                        individual_tags.update(tags_in_string)

                # Return sorted list of individual tags
                return sorted(list(individual_tags))

        except Exception as e:
            if attempt < max_retries - 1:
                # Wait before retrying
                time.sleep(0.5)
                continue
            else:
                # Final attempt failed, return empty list instead of showing error
                return []

    return []


def get_books_from_database(_engine):
    """Get list of unique book names from database with retry logic"""
    max_retries = 3
    for attempt in range(max_retries):
        try:
            with _engine.connect() as conn:
                result = conn.execute(
                    text(
                        "SELECT DISTINCT card_name FROM trello_time_tracking WHERE card_name IS NOT NULL ORDER BY card_name"
                    )
                )
                books = [row[0] for row in result]
                return books
        except Exception as e:
            if attempt < max_retries - 1:
                time.sleep(0.5)
                continue
            else:
                return []
    return []


def get_boards_from_database(_engine):
    """Get list of unique board names from database with retry logic"""
    max_retries = 3
    for attempt in range(max_retries):
        try:
            with _engine.connect() as conn:
                result = conn.execute(
                    text(
                        "SELECT DISTINCT board_name FROM trello_time_tracking WHERE board_name IS NOT NULL AND board_name != '' ORDER BY board_name"
                    )
                )
                boards = [row[0] for row in result]
                return boards
        except Exception as e:
            if attempt < max_retries - 1:
                time.sleep(0.5)
                continue
            else:
                return []
    return []


def emergency_stop_all_timers(engine):
    """Emergency function to stop all active timers and save progress when database connection fails"""
    try:
        # Initialize session state if needed
        if 'timers' not in st.session_state:
            st.session_state.timers = {}
        if 'timer_start_times' not in st.session_state:
            st.session_state.timer_start_times = {}

        saved_timers = 0
        current_time_utc = datetime.utcnow().replace(tzinfo=timezone.utc)
        current_time_bst = current_time_utc.astimezone(BST)

        # Process any active timers from session state
        for timer_key, is_active in st.session_state.timers.items():
            if is_active and timer_key in st.session_state.timer_start_times:
                try:
                    # Parse timer key to extract details
                    parts = timer_key.split('_')
                    if len(parts) >= 3:
                        card_name = '_'.join(parts[:-2])  # Reconstruct card name
                        list_name = parts[-2]
                        user_name = parts[-1]

                        # Calculate elapsed time using UTC-based function
                        start_time = st.session_state.timer_start_times[timer_key]
                        elapsed_seconds = calculate_timer_elapsed_time(start_time)

                        # Only save if significant time elapsed
                        if elapsed_seconds > 0:
                            # Try to save to database with retry logic
                            for attempt in range(3):
                                try:
                                    with engine.connect() as conn:
                                        # Save the time entry
                                        conn.execute(
                                            text(
                                                '''
                                            INSERT INTO trello_time_tracking
                                            (card_name, user_name, list_name, time_spent_seconds,
                                             date_started, session_start_time, board_name)
                                            VALUES (:card_name, :user_name, :list_name, :time_spent_seconds,
                                                   :date_started, :session_start_time, :board_name)
                                        '''
                                            ),
                                            {
                                                'card_name': card_name,
                                                'user_name': user_name,
                                                'list_name': list_name,
                                                'time_spent_seconds': elapsed_seconds,
                                                'date_started': start_time.date(),
                                                'session_start_time': start_time,
                                                'board_name': 'Manual Entry',
                                            },
                                        )

                                        # Remove from active timers table
                                        conn.execute(
                                            text('DELETE FROM active_timers WHERE timer_key = :timer_key'),
                                            {'timer_key': timer_key},
                                        )
                                        conn.commit()
                                        saved_timers += 1
                                        break
                                except Exception:
                                    if attempt == 2:  # Last attempt failed
                                        # Store in session state as backup
                                        if 'emergency_saved_times' not in st.session_state:
                                            st.session_state.emergency_saved_times = []
                                        st.session_state.emergency_saved_times.append(
                                            {
                                                'card_name': card_name,
                                                'user_name': user_name,
                                                'list_name': list_name,
                                                'elapsed_seconds': elapsed_seconds,
                                                'start_time': start_time,
                                            }
                                        )
                                    continue

                except Exception as e:
                    continue  # Skip this timer if parsing fails

        if saved_timers > 0:
            st.success(f"Successfully saved {saved_timers} active timer(s) before stopping.")

        # Try to clear active timers table if possible
        try:
            with engine.connect() as conn:
                conn.execute(text('DELETE FROM active_timers'))
                conn.commit()
        except Exception:
            pass  # Database might be completely unavailable

    except Exception as e:
        st.error(f"Emergency timer save failed: {str(e)}")


def recover_emergency_saved_times(engine):
    """Recover and save any emergency saved times from previous session"""
    if 'emergency_saved_times' in st.session_state and st.session_state.emergency_saved_times:
        saved_count = 0
        for saved_time in st.session_state.emergency_saved_times:
            try:
                with engine.connect() as conn:
                    conn.execute(
                        text(
                            '''
                        INSERT INTO trello_time_tracking
                        (card_name, user_name, list_name, time_spent_seconds,
                         date_started, session_start_time, board_name)
                        VALUES (:card_name, :user_name, :list_name, :time_spent_seconds,
                               :date_started, :session_start_time, :board_name)
                    '''
                        ),
                        {
                            'card_name': saved_time['card_name'],
                            'user_name': saved_time['user_name'],
                            'list_name': saved_time['list_name'],
                            'time_spent_seconds': saved_time['elapsed_seconds'],
                            'date_started': saved_time['start_time'].date(),
                            'session_start_time': saved_time['start_time'],
                            'board_name': 'Manual Entry',
                        },
                    )
                    conn.commit()
                    saved_count += 1
            except Exception:
                continue  # Skip if unable to save

        if saved_count > 0:
            st.success(f"Recovered {saved_count} emergency saved timer(s) from previous session.")

        # Clear the emergency saved times
        st.session_state.emergency_saved_times = []


def load_active_timers(engine):
    """Load active timers from database - simplified version"""
    try:
        with engine.connect() as conn:
            result = conn.execute(
                text(
                    '''
                SELECT timer_key, card_name, user_name, list_name, board_name,
                       start_time, accumulated_seconds, is_paused
                FROM active_timers
                ORDER BY start_time DESC
            '''
                )
            )

            active_timers = []
            for row in result:
                timer_key = row[0]
                card_name = row[1]
                user_name = row[2]
                list_name = row[3]
                board_name = row[4]
                start_time = row[5]
                accumulated_seconds = row[6] or 0
                is_paused = row[7] or False

                # Simple session state - just track if timer is running
                if 'timers' not in st.session_state:
                    st.session_state.timers = {}
                if 'timer_start_times' not in st.session_state:
                    st.session_state.timer_start_times = {}
                if 'timer_paused' not in st.session_state:
                    st.session_state.timer_paused = {}
                if 'timer_accumulated_time' not in st.session_state:
                    st.session_state.timer_accumulated_time = {}
                if 'timer_session_counts' not in st.session_state:
                    st.session_state.timer_session_counts = {}

                # Ensure timezone-aware datetime for consistency
                if start_time.tzinfo is None:
                    start_time_with_tz = start_time.replace(tzinfo=BST)
                else:
                    # Convert to BST for consistency in session state
                    start_time_with_tz = start_time.astimezone(BST)

                st.session_state.timers[timer_key] = True
                st.session_state.timer_start_times[timer_key] = start_time_with_tz
                st.session_state.timer_paused[timer_key] = is_paused
                st.session_state.timer_accumulated_time[timer_key] = accumulated_seconds
                st.session_state.timer_session_counts.setdefault(timer_key, 0)

                active_timers.append(
                    {
                        'timer_key': timer_key,
                        'card_name': card_name,
                        'user_name': user_name,
                        'list_name': list_name,
                        'board_name': board_name,
                        'start_time': start_time_with_tz,
                    }
                )

            return active_timers
    except Exception as e:
        error_msg = str(e)

        # Check if this is an SSL connection error indicating app restart
        if "SSL connection has been closed unexpectedly" in error_msg or "connection" in error_msg.lower():
            st.warning("App restarted - automatically stopping all active timers and saving progress...")

            # Try to recover and save any active timers from session state
            emergency_stop_all_timers(engine)

            # Clear session state timers since they've been saved
            if 'timers' in st.session_state:
                st.session_state.timers = {}
            if 'timer_start_times' in st.session_state:
                st.session_state.timer_start_times = {}

            return []
        else:
            st.error(f"Error loading active timers: {error_msg}")
            return []


def save_active_timer(
    engine,
    timer_key,
    card_name,
    user_name,
    list_name,
    board_name,
    start_time,
    accumulated_seconds=0,
    is_paused=False,
):
    """Save or update an active timer in the database."""
    try:
        with engine.connect() as conn:
            if start_time.tzinfo is None:
                start_time_with_tz = start_time.replace(tzinfo=BST)
            else:
                start_time_with_tz = start_time

            conn.execute(
                text(
                    '''
                INSERT INTO active_timers (timer_key, card_name, user_name, list_name,
                    board_name, start_time, accumulated_seconds, is_paused, created_at)
                VALUES (:timer_key, :card_name, :user_name, :list_name, :board_name,
                    :start_time, :accumulated_seconds, :is_paused, CURRENT_TIMESTAMP)
                ON CONFLICT (timer_key) DO UPDATE SET
                    start_time = EXCLUDED.start_time,
                    accumulated_seconds = EXCLUDED.accumulated_seconds,
                    is_paused = EXCLUDED.is_paused,
                    created_at = CURRENT_TIMESTAMP
            '''
                ),
                {
                    'timer_key': timer_key,
                    'card_name': card_name,
                    'user_name': user_name,
                    'list_name': list_name,
                    'board_name': board_name,
                    'start_time': start_time_with_tz,
                    'accumulated_seconds': accumulated_seconds,
                    'is_paused': is_paused,
                },
            )
            conn.commit()
    except Exception as e:
        st.error(f"Error saving active timer: {str(e)}")


def update_active_timer_state(
    engine, timer_key, accumulated_seconds, is_paused, start_time=None
):
    """Update active timer pause/resume state."""
    try:
        with engine.connect() as conn:
            params = {
                'accumulated_seconds': accumulated_seconds,
                'is_paused': is_paused,
                'timer_key': timer_key,
            }
            if start_time is not None:
                if start_time.tzinfo is None:
                    start_time_with_tz = start_time.replace(tzinfo=BST)
                else:
                    start_time_with_tz = start_time
                params['start_time'] = start_time_with_tz
                conn.execute(
                    text(
                        '''
                    UPDATE active_timers
                    SET accumulated_seconds = :accumulated_seconds,
                        is_paused = :is_paused,
                        start_time = :start_time
                    WHERE timer_key = :timer_key
                '''
                    ),
                    params,
                )
            else:
                conn.execute(
                    text(
                        '''
                    UPDATE active_timers
                    SET accumulated_seconds = :accumulated_seconds,
                        is_paused = :is_paused
                    WHERE timer_key = :timer_key
                '''
                    ),
                    params,
                )
            conn.commit()
    except Exception as e:
        st.error(f"Error updating active timer: {str(e)}")


def remove_active_timer(engine, timer_key):
    """Remove active timer from database"""
    try:
        with engine.connect() as conn:
            conn.execute(
                text(
                    '''
                DELETE FROM active_timers WHERE timer_key = :timer_key
            '''
                ),
                {'timer_key': timer_key},
            )
            conn.commit()
    except Exception as e:
        st.error(f"Error removing active timer: {str(e)}")


def stop_active_timer(engine, timer_key):
    """Stop a running timer and save its elapsed time."""
    if timer_key not in st.session_state.get('timers', {}):
        return

    start_time = st.session_state.timer_start_times.get(timer_key)
    accumulated = st.session_state.timer_accumulated_time.get(timer_key, 0)
    paused = st.session_state.timer_paused.get(timer_key, False)

    elapsed_seconds = accumulated
    if not paused and start_time:
        elapsed_seconds += calculate_timer_elapsed_time(start_time)

    parts = timer_key.split('_')
    if len(parts) < 3:
        return

    card_name = '_'.join(parts[:-2])
    list_name = parts[-2]
    user_name = parts[-1]

    board_name = 'Manual Entry'
    try:
        with engine.connect() as conn:
            res = conn.execute(
                text('SELECT board_name FROM active_timers WHERE timer_key = :timer_key'), {'timer_key': timer_key}
            )
            row = res.fetchone()
            if row and row[0]:
                board_name = row[0]
    except Exception:
        pass

    try:
        with engine.connect() as conn:
            conn.execute(
                text(
                    '''
                INSERT INTO trello_time_tracking
                (card_name, user_name, list_name, time_spent_seconds,
                 date_started, session_start_time, board_name)
                VALUES (:card_name, :user_name, :list_name, :time_spent_seconds,
                        :date_started, :session_start_time, :board_name)
                ON CONFLICT (card_name, user_name, list_name, date_started, time_spent_seconds)
                DO UPDATE SET
                    session_start_time = EXCLUDED.session_start_time,
                    board_name = EXCLUDED.board_name,
                    created_at = CURRENT_TIMESTAMP
            '''
                ),
                {
                    'card_name': card_name,
                    'user_name': user_name,
                    'list_name': list_name,
                    'time_spent_seconds': elapsed_seconds,
                    'date_started': (start_time or datetime.now(BST)).date(),
                    'session_start_time': start_time or datetime.now(BST),
                    'board_name': board_name,
                },
            )
            conn.execute(text('DELETE FROM active_timers WHERE timer_key = :timer_key'), {'timer_key': timer_key})
            conn.commit()
    except Exception as e:
        st.error(f"Error saving timer data: {str(e)}")

    st.session_state.timers[timer_key] = False
    if timer_key in st.session_state.timer_start_times:
        del st.session_state.timer_start_times[timer_key]
    if timer_key in st.session_state.timer_accumulated_time:
        del st.session_state.timer_accumulated_time[timer_key]
    if timer_key in st.session_state.timer_paused:
        del st.session_state.timer_paused[timer_key]
    st.session_state.setdefault('timer_session_counts', {})
    st.session_state.timer_session_counts[timer_key] = st.session_state.timer_session_counts.get(timer_key, 0) + 1
    st.rerun()


def display_active_timers_sidebar(engine):
    """Display running timers in the sidebar on every page."""
    active_timer_count = sum(1 for running in st.session_state.timers.values() if running)
    with st.sidebar:
        st.write(f"**Active Timers ({active_timer_count})**")
        if active_timer_count == 0:
            st.write("No active timers")
        else:
            for task_key, is_running in st.session_state.timers.items():
                if is_running and task_key in st.session_state.timer_start_times:
                    parts = task_key.split('_')
                    if len(parts) >= 3:
                        book_title = '_'.join(parts[:-2])
                        stage_name = parts[-2]
                        user_name = parts[-1]
                        start_time = st.session_state.timer_start_times[task_key]
                        accumulated = st.session_state.timer_accumulated_time.get(task_key, 0)
                        paused = st.session_state.timer_paused.get(task_key, False)
                        current_elapsed = 0 if paused else calculate_timer_elapsed_time(start_time)
                        elapsed_seconds = accumulated + current_elapsed
                        elapsed_str = format_seconds_to_time(elapsed_seconds)

                        estimate_seconds = get_task_estimate(engine, book_title, user_name, stage_name)
                        estimate_str = format_seconds_to_time(estimate_seconds)

                        user_display = user_name if user_name and user_name != "Not set" else "Unassigned"

                        col1, col2, col3 = st.columns([3, 1, 1])
                        with col1:
                            status_text = "PAUSED" if paused else "RECORDING"
                            sidebar_timer_id = f"sidebar_timer_{task_key}"
                            components.html(
                                f"""
<style>
body {{
  font-family: 'Noto Sans', sans-serif;
  margin: 0;
}}

.timer-text {{
  white-space: normal;
  word-break: break-word;
  font-weight: bold;
}}
</style>
<div id='{sidebar_timer_id}' class='timer-text'>{book_title} - {stage_name}<br>{user_display}<br>{elapsed_str}/{estimate_str} - {status_text}</div>
<script>
var elem = document.getElementById('{sidebar_timer_id}');
function updateThemeStyles() {{
  var parentStyles = window.parent.getComputedStyle(window.parent.document.body);
  elem.style.fontFamily = parentStyles.getPropertyValue('font-family');
  elem.style.color = parentStyles.getPropertyValue('color');
}}
updateThemeStyles();
setInterval(updateThemeStyles, 1000);

var elapsed = {elapsed_seconds};
var paused = {str(paused).lower()};
function fmt(sec) {{
  var h = Math.floor(sec / 3600).toString().padStart(2, '0');
  var m = Math.floor((sec % 3600) / 60).toString().padStart(2, '0');
  var s = Math.floor(sec % 60).toString().padStart(2, '0');
  return h + ':' + m + ':' + s;
}}
function resizeIframe() {{
  var iframe = window.frameElement;
  if (iframe) {{
    iframe.style.height = (document.body.scrollHeight + 4) + 'px';

  }}
}}
resizeIframe();
if (!paused) {{
  setInterval(function() {{
    elapsed += 1;
    elem.innerHTML = "{book_title} - {stage_name}<br>{user_display}<br>" + fmt(elapsed) + "/{estimate_str} - {status_text}";
    resizeIframe();
  }}, 1000);
}}
</script>
""",
                                height=0,
                            )
                        with col2:
                            pause_label = "Resume" if paused else "Pause"
                            if st.button(pause_label, key=f"summary_pause_{task_key}"):
                                if paused:
                                    resume_time = datetime.utcnow().replace(tzinfo=timezone.utc).astimezone(BST)
                                    st.session_state.timer_start_times[task_key] = resume_time
                                    st.session_state.timer_paused[task_key] = False
                                    update_active_timer_state(engine, task_key, accumulated, False, resume_time)
                                else:
                                    elapsed_since_start = calculate_timer_elapsed_time(start_time)
                                    new_accum = accumulated + elapsed_since_start
                                    st.session_state.timer_accumulated_time[task_key] = new_accum
                                    st.session_state.timer_paused[task_key] = True
                                    update_active_timer_state(engine, task_key, new_accum, True)
                                st.rerun()
                        with col3:
                            if st.button("Stop", key=f"summary_stop_{task_key}"):
                                stop_active_timer(engine, task_key)

        st.markdown("---")


def update_task_completion(engine, card_name, user_name, list_name, completed):
    """Update task completion status for all matching records"""
    try:
        with engine.connect() as conn:
            # Update all matching records and get count of affected rows
            result = conn.execute(
                text(
                    """
                UPDATE trello_time_tracking
                SET completed = :completed
                WHERE card_name = :card_name
                AND COALESCE(user_name, 'Not set') = :user_name
                AND list_name = :list_name
                AND archived = FALSE
            """
                ),
                {'completed': completed, 'card_name': card_name, 'user_name': user_name, 'list_name': list_name},
            )
            conn.commit()

            # Verify the update worked
            rows_affected = result.rowcount
            if rows_affected == 0:
                st.warning(f"No records found to update for {card_name} - {list_name} ({user_name})")

    except Exception as e:
        st.error(f"Error updating task completion: {str(e)}")


def get_task_completion(engine, card_name, user_name, list_name):
    """Get task completion status"""
    try:
        with engine.connect() as conn:
            result = conn.execute(
                text(
                    """
                SELECT completed FROM trello_time_tracking
                WHERE card_name = :card_name
                AND COALESCE(user_name, 'Not set') = :user_name
                AND list_name = :list_name
                LIMIT 1
            """
                ),
                {'card_name': card_name, 'user_name': user_name, 'list_name': list_name},
            )
            row = result.fetchone()
            return row[0] if row else False
    except Exception as e:
        st.error(f"Error getting task completion: {str(e)}")
        return False


def get_task_estimate(engine, card_name, user_name, list_name):
    """Return estimated time for a task in seconds."""

    try:
        with engine.connect() as conn:
            result = conn.execute(
                text(
                    '''
                SELECT MAX(card_estimate_seconds)

                FROM trello_time_tracking
                WHERE card_name = :card_name
                AND list_name = :list_name
                AND COALESCE(user_name, 'Not set') = :user_name
                AND archived = FALSE
            '''
                ),
                {
                    'card_name': card_name,
                    'list_name': list_name,
                    'user_name': user_name,
                },
            )
            row = result.fetchone()
            return int(row[0]) if row and row[0] else 0
    except Exception as e:
        st.error(f"Error getting task estimate: {str(e)}")

        return 0


def check_all_tasks_completed(engine, card_name):
    """Check if all tasks for a book are completed"""
    try:
        with engine.connect() as conn:
            # Get all tasks for this book - need to check each user/stage combination
            result = conn.execute(
                text(
                    """
                SELECT list_name, COALESCE(user_name, 'Not set') as user_name,
                       BOOL_AND(COALESCE(completed, false)) as all_completed
                FROM trello_time_tracking
                WHERE card_name = :card_name
                AND archived = FALSE
                GROUP BY list_name, COALESCE(user_name, 'Not set')
            """
                ),
                {'card_name': card_name},
            )

            task_groups = result.fetchall()
            if not task_groups:
                return False

            # Check if all task groups are completed
            for task_group in task_groups:
                if not task_group[2]:  # all_completed column
                    return False

            return True
    except Exception as e:
        st.error(f"Error checking book completion: {str(e)}")
        return False


def delete_task_stage(engine, card_name, user_name, list_name):
    """Delete a specific task stage from the database"""
    try:
        with engine.connect() as conn:
            conn.execute(
                text(
                    """
                DELETE FROM trello_time_tracking
                WHERE card_name = :card_name
                AND COALESCE(user_name, 'Not set') = :user_name
                AND list_name = :list_name
            """
                ),
                {'card_name': card_name, 'user_name': user_name, 'list_name': list_name},
            )
            conn.commit()
            return True
    except Exception as e:
        st.error(f"Error deleting task stage: {str(e)}")
        return False


def create_book_record(engine, card_name, board_name=None, tag=None):
    """Create a book record in the books table"""
    try:
        with engine.connect() as conn:
            conn.execute(
                text(
                    """
                INSERT INTO books (card_name, board_name, tag)
                VALUES (:card_name, :board_name, :tag)
                ON CONFLICT (card_name) DO UPDATE SET
                    board_name = EXCLUDED.board_name,
                    tag = EXCLUDED.tag
            """
                ),
                {'card_name': card_name, 'board_name': board_name, 'tag': tag},
            )
            conn.commit()
            return True
    except Exception as e:
        st.error(f"Error creating book record: {str(e)}")
        return False


def get_all_books(engine):
    """Get all books from the books table, including those without tasks"""
    try:
        with engine.connect() as conn:
            result = conn.execute(
                text(
                    """
                SELECT DISTINCT card_name, board_name, tag
                FROM books
                WHERE archived = FALSE
                UNION
                SELECT DISTINCT card_name, board_name, tag
                FROM trello_time_tracking
                WHERE archived = FALSE
                ORDER BY card_name
            """
                )
            )
            return result.fetchall()
    except Exception as e:
        st.error(f"Error fetching books: {str(e)}")
        return []


def get_available_stages_for_book(engine, card_name):
    """Get stages not yet associated with a book"""
    all_stages = [
        "Editorial R&D",
        "Editorial Writing",
        "1st Edit",
        "2nd Edit",
        "Design R&D",
        "In Design",
        "1st Proof",
        "2nd Proof",
        "Editorial Sign Off",
        "Design Sign Off",
    ]

    try:
        with engine.connect() as conn:
            result = conn.execute(
                text(
                    """
                SELECT DISTINCT list_name
                FROM trello_time_tracking
                WHERE card_name = :card_name AND archived = FALSE
            """
                ),
                {'card_name': card_name},
            )

            existing_stages = [row[0] for row in result.fetchall()]
            available_stages = [stage for stage in all_stages if stage not in existing_stages]
            return available_stages
    except Exception as e:
        st.error(f"Error getting available stages: {str(e)}")
        return []


def add_stage_to_book(engine, card_name, stage_name, board_name=None, tag=None, estimate_seconds=3600):
    """Add a new stage to a book"""
    try:
        with engine.connect() as conn:
            conn.execute(
                text(
                    """
                INSERT INTO trello_time_tracking
                (card_name, user_name, list_name, time_spent_seconds, card_estimate_seconds, board_name, created_at, tag)
                VALUES (:card_name, :user_name, :list_name, :time_spent_seconds, :card_estimate_seconds, :board_name, :created_at, :tag)
            """
                ),
                {
                    'card_name': card_name,
                    'user_name': 'Not set',  # Unassigned initially
                    'list_name': stage_name,
                    'time_spent_seconds': 0,
                    'card_estimate_seconds': estimate_seconds,
                    'board_name': board_name,
                    'created_at': datetime.now(BST),
                    'tag': tag,
                },
            )
            conn.commit()
            return True
    except Exception as e:
        st.error(f"Error adding stage: {str(e)}")
        return False


def import_books_from_csv(engine, df):
    """Import books and stage estimates from a CSV DataFrame"""
    required_cols = {"Card Name", "Board", "Tags"}
    if not required_cols.issubset(df.columns):
        missing = required_cols - set(df.columns)
        return False, f"Missing columns: {', '.join(missing)}"

    # Identify stage columns (user and time pairs)
    stage_names = [col for col in df.columns if col not in required_cols and not col.endswith(" Time")]
    if not stage_names:
        return False, "No stage columns found in CSV"

    total_entries = 0

    for _, row in df.iterrows():
        card_name = str(row.get("Card Name", "")).strip()
        if not card_name:
            card_name = "Not set"
        board_name = row.get("Board")
        board_name = str(board_name).strip() if pd.notna(board_name) else None
        tag_value = row.get("Tags")
        if pd.notna(tag_value) and str(tag_value).strip():
            final_tag = ", ".join([t.strip() for t in str(tag_value).split(",") if t.strip()])
        else:
            final_tag = None

        # Create/update book record
        create_book_record(engine, card_name, board_name, final_tag)

        current_time = datetime.now(BST)

        with engine.connect() as conn:
            for stage in stage_names:
                time_col = f"{stage} Time"
                if time_col not in df.columns:
                    continue

                time_val = row.get(time_col)
                if pd.isna(time_val) or str(time_val).strip() == "":
                    continue

                try:
                    hours = parse_hours_minutes(time_val)
                except Exception:
                    continue
                if hours <= 0:
                    continue

                estimate_seconds = int(round(hours * 60)) * 60

                user_val = row.get(stage)
                if pd.notna(user_val):
                    final_user = normalize_user_name(user_val)
                else:
                    final_user = "Not set"

                def stable_hash(*values) -> str:
                    """Return a short deterministic hash for key uniqueness."""
                    s = "||".join("" if v is None else str(v) for v in values)
                    return hashlib.md5(s.encode()).hexdigest()[:8]
                
                conn.execute(
                    text(
                        '''
                    INSERT INTO trello_time_tracking
                    (card_name, user_name, list_name, time_spent_seconds,
                     card_estimate_seconds, board_name, created_at,
                     session_start_time, tag)
                    VALUES (:card_name, :user_name, :list_name, :time_spent_seconds,
                            :card_estimate_seconds, :board_name, :created_at,
                            :session_start_time, :tag)
                    '''
                    ),
                    {
                        'card_name': card_name,
                        'user_name': final_user,
                        'list_name': stage,
                        'time_spent_seconds': 0,
                        'card_estimate_seconds': estimate_seconds,
                        'board_name': board_name,
                        'created_at': current_time,
                        'session_start_time': None,
                        'tag': final_tag,
                    },
                )
                total_entries += 1

            conn.commit()

    return True, f"Imported {total_entries} stage entries from CSV"


def get_filtered_tasks_from_database(
    _engine, user_name=None, book_name=None, board_name=None, tag_name=None, start_date=None, end_date=None
):
    """Get filtered tasks from database with multiple filter options"""
    try:
        query = '''
            WITH task_summary AS (
                SELECT card_name, list_name, COALESCE(user_name, 'Not set') as user_name, board_name, tag,
                       SUM(time_spent_seconds) as total_time,
                       MAX(card_estimate_seconds) as estimated_seconds,
                       MIN(CASE WHEN session_start_time IS NOT NULL THEN session_start_time END) as first_session
                FROM trello_time_tracking
                WHERE 1=1
        '''
        params = {}

        # Add filters based on provided parameters
        if user_name and user_name != "All Users":
            query += ' AND COALESCE(user_name, \'Not set\') = :user_name'
            params['user_name'] = user_name

        if book_name and book_name != "All Books":
            query += ' AND card_name = :book_name'
            params['book_name'] = book_name

        if board_name and board_name != "All Boards":
            query += ' AND board_name = :board_name'
            params['board_name'] = board_name

        if tag_name and tag_name != "All Tags":
            query += ' AND (tag = :tag_name OR tag LIKE :tag_name_pattern1 OR tag LIKE :tag_name_pattern2 OR tag LIKE :tag_name_pattern3)'
            params['tag_name'] = tag_name
            params['tag_name_pattern1'] = f'{tag_name},%'  # Tag at start
            params['tag_name_pattern2'] = f'%, {tag_name},%'  # Tag in middle
            params['tag_name_pattern3'] = f'%, {tag_name}'  # Tag at end

        query += '''
                GROUP BY card_name, list_name, COALESCE(user_name, 'Not set'), board_name, tag
            )
            SELECT card_name, list_name, user_name, board_name, tag, first_session, total_time, estimated_seconds
            FROM task_summary
        '''

        # Add date filtering to the main query if needed
        if start_date or end_date:
            date_conditions = []
            if start_date:
                date_conditions.append('first_session >= :start_date')
                params['start_date'] = start_date
            if end_date:
                date_conditions.append('first_session <= :end_date')
                params['end_date'] = end_date

            if date_conditions:
                query += ' WHERE ' + ' AND '.join(date_conditions)

        query += ' ORDER BY first_session DESC, card_name, list_name'

        with _engine.connect() as conn:
            result = conn.execute(text(query), params)
            data = []
            for row in result:
                card_name = row[0]
                list_name = row[1]
                user_name = row[2]
                board_name = row[3]
                tag = row[4]
                first_session = row[5]
                total_time = row[6]
                estimated_time = row[7] if row[7] else 0

                if first_session:
                    # Format as DD/MM/YYYY HH:MM
                    date_time_str = first_session.strftime('%d/%m/%Y %H:%M')
                else:
                    date_time_str = 'Manual Entry'

                # Calculate completion percentage
                if estimated_time > 0:
                    completion_ratio = total_time / estimated_time
                    if completion_ratio <= 1.0:
                        completion_percentage = f"{int(completion_ratio * 100)}%"
                    else:
                        over_percentage = int((completion_ratio - 1.0) * 100)
                        completion_percentage = f"{over_percentage}% over"
                else:
                    completion_percentage = "No estimate"

                data.append(
                    {
                        'Book Title': card_name,
                        'Stage': list_name,
                        'User': user_name,
                        'Board': board_name,
                        'Tag': tag if tag else 'No Tag',
                        'Session Started': date_time_str,
                        'Time Allocation': format_seconds_to_time(estimated_time) if estimated_time > 0 else 'Not Set',
                        'Time Spent': format_seconds_to_time(total_time),
                        'Completion %': completion_percentage,
                    }
                )
            return pd.DataFrame(data)
    except Exception as e:
        st.error(f"Error fetching user tasks: {str(e)}")
        return pd.DataFrame()


def format_seconds_to_time(seconds):
    """Convert seconds to hh:mm:ss format"""
    if pd.isna(seconds) or seconds == 0:
        return "00:00:00"

    # Convert to integer to handle any float values
    seconds = int(seconds)
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    secs = seconds % 60
    return f"{hours:02d}:{minutes:02d}:{secs:02d}"


def render_basic_js_timer(timer_id, status_label, elapsed_seconds, paused):
    """Render a simple JavaScript-based timer."""
    elapsed_str = format_seconds_to_time(elapsed_seconds)
    return f"""
<style>
body {{ font-family: 'Noto Sans', sans-serif; }}
</style>
<div id='{timer_id}'><strong>{status_label}</strong> ({elapsed_str})</div>
<script>
var elem = document.getElementById('{timer_id}');
function updateThemeStyles() {{
  var parentStyles = window.parent.getComputedStyle(window.parent.document.body);
  elem.style.fontFamily = parentStyles.getPropertyValue('font-family');
  elem.style.color = parentStyles.getPropertyValue('color');
}}
updateThemeStyles();
setInterval(updateThemeStyles, 1000);

var elapsed = {elapsed_seconds};
var paused = {str(paused).lower()};
function fmt(sec) {{
  var h = Math.floor(sec / 3600).toString().padStart(2, '0');
  var m = Math.floor((sec % 3600) / 60).toString().padStart(2, '0');
  var s = Math.floor(sec % 60).toString().padStart(2, '0');
  return h + ':' + m + ':' + s;
}}
if (!paused) {{
  setInterval(function() {{
    elapsed += 1;
    elem.innerHTML = "<strong>{status_label}</strong> (" + fmt(elapsed) + ")";
  }}, 1000);
}}
</script>
"""


def parse_hours_minutes(value):
    """Parse HH:MM or decimal hour strings to float hours."""
    if value is None or value == "":
        return 0.0

    try:
        if isinstance(value, (int, float)):
            return float(value)

        value = str(value).strip()

        if ":" in value:
            parts = value.split(":")
            if len(parts) == 2:
                hours = float(parts[0])
                minutes = float(parts[1])
                if minutes >= 60:
                    st.warning("Minutes must be less than 60")
                    return 0.0
                return hours + minutes / 60

        return float(value)
    except ValueError:
        st.warning("Use HH:MM or decimal hours (e.g., 2:30)")
        return 0.0


def calculate_timer_elapsed_time(start_time):
    """Calculate elapsed time from start_time to now using UTC for accuracy"""
    if not start_time:
        return 0

    # Use UTC for all calculations to avoid timezone issues
    current_time_utc = datetime.utcnow().replace(tzinfo=timezone.utc)

    # Convert start_time to UTC
    if start_time.tzinfo is None:
        # Assume start_time is in BST if no timezone info
        start_time = start_time.replace(tzinfo=BST).astimezone(timezone.utc)
    else:
        # Convert to UTC
        start_time = start_time.astimezone(timezone.utc)

    elapsed = current_time_utc - start_time
    return max(0, int(elapsed.total_seconds()))  # Ensure non-negative result


def calculate_completion_status(time_spent_seconds, estimated_seconds):
    """Calculate completion status based on time spent vs estimated time"""
    if pd.isna(estimated_seconds) or estimated_seconds == 0:
        return "No estimate"

    completion_ratio = time_spent_seconds / estimated_seconds

    if completion_ratio <= 1.0:
        percentage = int(completion_ratio * 100)
        return f"{percentage}% Complete"
    else:
        over_percentage = int((completion_ratio - 1.0) * 100)
        return f"{over_percentage}% over allocation"


@st.cache_data(ttl=60)
def process_book_summary(df):
    """Generate Book Summary Table"""
    try:
        grouped = df.groupby('Card name')

        total_time = grouped['Time spent (s)'].sum()
        estimated = grouped['Card estimate(s)'].max()
        boards = grouped['Board'].first()

        def get_main_user(group):
            user_totals = group.groupby('User')['Time spent (s)'].sum()
            return user_totals.idxmax() if not user_totals.empty else "Unknown"

        main_user_series = grouped.apply(get_main_user)

        completion_list = [
            calculate_completion_status(t, 0 if pd.isna(e) else e) for t, e in zip(total_time, estimated)
        ]

        df_summary = pd.DataFrame(
            {
                'Book Title': total_time.index,
                'Board': boards.values,
                'Main User': main_user_series.values,
                'Time Spent': total_time.apply(format_seconds_to_time).values,
                'Estimated Time': estimated.fillna(0).apply(format_seconds_to_time).values,
                'Completion': completion_list,
            }
        )

        return df_summary.reset_index(drop=True)

    except Exception as e:
        st.error(f"Error processing book summary: {str(e)}")
        return pd.DataFrame()


def get_most_recent_activity(df, card_name):
    """Get the most recent list/stage worked on for a specific card"""
    try:
        card_data = df[df['Card name'] == card_name]

        if card_data.empty:
            return "Unknown"

        # If Date started (f) exists, use it to find most recent
        if 'Date started (f)' in df.columns and not card_data['Date started (f)'].isna().all():
            # Convert dates and find the most recent entry
            card_data_with_dates = card_data.dropna(subset=['Date started (f)'])
            if not card_data_with_dates.empty:
                card_data_with_dates = card_data_with_dates.copy()
                card_data_with_dates['parsed_date'] = pd.to_datetime(
                    card_data_with_dates['Date started (f)'], format='%m/%d/%Y', errors='coerce'
                )
                card_data_with_dates = card_data_with_dates.dropna(subset=['parsed_date'])
                if not card_data_with_dates.empty:
                    most_recent = card_data_with_dates.loc[card_data_with_dates['parsed_date'].idxmax()]
                    return most_recent['List']

        # Fallback: return the last entry (by order in CSV)
        return card_data.iloc[-1]['List']
    except Exception as e:
        return "Unknown"


def create_progress_bar_html(completion_percentage):
    """Create HTML progress bar for completion status"""
    if completion_percentage <= 100:
        # Normal progress (green)
        width = min(completion_percentage, 100)
        color = "#2AA395"  # Updated progress colour
        return f"""
        <div style="margin-bottom: 5px;">
            <div style="background-color: #f0f0f0; border-radius: 10px; padding: 2px; width: 200px; height: 20px;">
                <div style="background-color: {color}; width: {width}%; height: 16px; border-radius: 8px;"></div>
            </div>
            <div style="font-size: 12px; font-weight: bold; color: {color}; text-align: center;">
                {completion_percentage:.1f}% complete
            </div>
        </div>
        """
    else:
        # Over allocation (red with overflow)
        over_percentage = completion_percentage - 100
        return f"""
        <div style="margin-bottom: 5px;">
            <div style="background-color: #f0f0f0; border-radius: 10px; padding: 2px; width: 200px; height: 20px;">
                <div style="background-color: #dc3545; width: 100%; height: 16px; border-radius: 8px;"></div>
            </div>
            <div style="font-size: 12px; font-weight: bold; color: #dc3545; text-align: center;">
                {over_percentage:.1f}% over allocation
            </div>
        </div>
        """


def process_book_completion(df, search_filter=None):
    """Generate Book Completion Table with visual progress"""
    try:
        # Apply search filter if provided
        if search_filter:
            # Escape special regex characters to handle punctuation properly
            escaped_filter = re.escape(search_filter)
            df = df[df['Card name'].str.contains(escaped_filter, case=False, na=False)]

        if df.empty:
            return pd.DataFrame()

        # Group by book title (Card name)
        book_groups = df.groupby('Card name')

        book_completion_data = []

        for book_title, group in book_groups:
            # Calculate total time spent
            total_time_spent = group['Time spent (s)'].sum()

            # Get estimated time (assuming it's the same for all rows of the same book)
            estimated_time = 0
            if 'Card estimate(s)' in group.columns and len(group) > 0:
                est_val = group['Card estimate(s)'].iloc[0]
                if not pd.isna(est_val):
                    estimated_time = est_val

            # Get most recent activity
            most_recent_list = get_most_recent_activity(df, book_title)

            # Calculate completion status
            completion = calculate_completion_status(total_time_spent, estimated_time)

            # Create visual progress element
            if estimated_time > 0:
                completion_percentage = (total_time_spent / estimated_time) * 100
                progress_bar_html = create_progress_bar_html(completion_percentage)
            else:
                progress_bar_html = '<div style="font-style: italic; color: #666;">No estimate</div>'

            visual_progress = f"""
            <div style="padding: 10px; border: 1px solid #ddd; border-radius: 8px; margin: 2px 0; background-color: #fafafa;">
                <div style="font-weight: bold; font-size: 14px; margin-bottom: 5px; color: #000;">{book_title}</div>
                <div style="font-size: 12px; color: #666; margin-bottom: 8px;">Current stage: {most_recent_list}</div>
                <div>{progress_bar_html}</div>
            </div>
            """

            book_completion_data.append(
                {
                    'Book Title': book_title,
                    'Visual Progress': visual_progress,
                }
            )

        return pd.DataFrame(book_completion_data)

    except Exception as e:
        st.error(f"Error processing book completion: {str(e)}")
        return pd.DataFrame()


def convert_date_format(date_str):
    """Convert date from mm/dd/yyyy format to dd/mm/yyyy format"""
    try:
        if pd.isna(date_str) or date_str == 'N/A':
            return 'N/A'

        # Parse the date string - handle both with and without time
        if ' ' in str(date_str):
            # Has time component
            date_part, time_part = str(date_str).split(' ', 1)
            date_obj = datetime.strptime(date_part, '%m/%d/%Y')
            return f"{date_obj.strftime('%d/%m/%Y')} {time_part}"
        else:
            # Date only
            date_obj = datetime.strptime(str(date_str), '%m/%d/%Y')
            return date_obj.strftime('%d/%m/%Y')
    except:
        return str(date_str)  # Return original if conversion fails


def process_user_task_breakdown(df):
    """Generate User Task Breakdown Table with aggregated time"""
    try:
        # Check if Date started column exists in the CSV
        has_date = 'Date started (f)' in df.columns

        if has_date:
            # Convert date format from mm/dd/yyyy to datetime for proper sorting
            df_copy = df.copy()

            # Try multiple date formats to handle different possible formats
            df_copy['Date_parsed'] = pd.to_datetime(df_copy['Date started (f)'], errors='coerce')

            # If initial parsing failed, try specific formats
            if df_copy['Date_parsed'].isna().all():
                # Try mm/dd/yyyy format without time
                df_copy['Date_parsed'] = pd.to_datetime(df_copy['Date started (f)'], format='%m/%d/%Y', errors='coerce')

            # Group by User, Book Title, and List to aggregate multiple sessions
            # For each group, sum the time and take the earliest date
            agg_funcs = {
                'Time spent (s)': 'sum',
                'Date_parsed': 'min',  # Get earliest date
                'Date started (f)': 'first',  # Keep original format for fallback
            }

            aggregated = df_copy.groupby(['User', 'Card name', 'List']).agg(agg_funcs).reset_index()

            # Convert the earliest date back to dd/mm/yyyy format for display (date only, no time)
            def format_date_display(date_val):
                if pd.notna(date_val):
                    return date_val.strftime('%d/%m/%Y')
                else:
                    return 'N/A'

            aggregated['Date_display'] = aggregated['Date_parsed'].apply(format_date_display)

            # Rename columns for clarity
            aggregated = aggregated[['User', 'Card name', 'List', 'Date_display', 'Time spent (s)']]
            aggregated.columns = ['User', 'Book Title', 'List', 'Date', 'Time Spent (s)']

        else:
            # Group by User, Book Title (Card name), and List (stage/task)
            # Aggregate time spent for duplicate combinations
            aggregated = df.groupby(['User', 'Card name', 'List'])['Time spent (s)'].sum().reset_index()

            # Rename columns for clarity
            aggregated.columns = ['User', 'Book Title', 'List', 'Time Spent (s)']

            # Add empty Date column if not present
            aggregated['Date'] = 'N/A'

        # Format time spent
        aggregated['Time Spent'] = aggregated['Time Spent (s)'].apply(format_seconds_to_time)

        # Drop the seconds column as we now have formatted time
        aggregated = aggregated.drop('Time Spent (s)', axis=1)

        # Reorder columns to put Date after List
        aggregated = aggregated[['User', 'Book Title', 'List', 'Date', 'Time Spent']]

        # Sort by User → Book Title → List
        aggregated = aggregated.sort_values(['User', 'Book Title', 'List'])

        return aggregated.reset_index(drop=True)

    except Exception as e:
        st.error(f"Error processing user task breakdown: {str(e)}")
        return pd.DataFrame()


def main():
    # Initialise database connection
    engine = init_database()
    if not engine:
        st.error("Could not connect to database. Please check your configuration.")
        return

    st.title("Book Production Time Tracking")
    st.markdown("Track time spent on different stages of book production with detailed stage-specific analysis.")

    # Database already initialized earlier

    # Initialize timer session state
    if 'timers' not in st.session_state:
        st.session_state.timers = {}
    if 'timer_start_times' not in st.session_state:
        st.session_state.timer_start_times = {}
    if 'timer_paused' not in st.session_state:
        st.session_state.timer_paused = {}
    if 'timer_accumulated_time' not in st.session_state:
        st.session_state.timer_accumulated_time = {}
    if 'timer_session_counts' not in st.session_state:
        st.session_state.timer_session_counts = {}

    # Recover any emergency saved times from previous session
    recover_emergency_saved_times(engine)

    # Load and restore active timers from database on every page load
    # This ensures timers are always properly restored even if session state is lost
    active_timers = load_active_timers(engine)
    if active_timers and 'timers_loaded' not in st.session_state:
        st.info(f"Restored {len(active_timers)} active timer(s) from previous session.")
        st.session_state.timers_loaded = True

    # Show active timers in sidebar regardless of selected tab
    display_active_timers_sidebar(engine)

    # Create tabs for different views as a horizontal selection
    tab_names = ["Book Progress", "Add Book", "Archive", "Reporting", "Error Log"]
    (
        book_progress_tab,
        add_book_tab,
        archive_tab,
        reporting_tab,
        error_log_tab,
    ) = st.tabs(tab_names)

    # Divider below the tab selector
    st.markdown("---")


    # Create content for each tab
    with add_book_tab:
        st.header("Upload CSV")
        st.markdown(
            "Upload a CSV file with columns 'Card Name', 'Board', 'Tags' followed by stage/user and 'Stage Time' pairs."
        )
        uploaded_csv = st.file_uploader("Choose CSV file", type="csv", key="csv_upload")
        if uploaded_csv is not None:
            # Limit file size to 5MB
            max_size = 5 * 1024 * 1024  # 5MB in bytes
            if uploaded_csv.size > max_size:
                st.error("File size exceeds 5MB limit")
            else:
                try:
                    csv_df = pd.read_csv(uploaded_csv)
                    success, msg = import_books_from_csv(engine, csv_df)
                    if success:
                        st.success(msg)
                    else:
                        st.error(msg)
                except Exception as e:
                    st.error(f"Error reading CSV: {str(e)}")

        with open("time_tracker_example.csv", "rb") as example_file:

            st.download_button(
                label="Download example csv format",
                data=example_file,
                file_name="time_tracker_example.csv",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
        st.markdown("---")

        # Manual Data Entry Form
        st.header("Manual Data Entry")
        st.markdown("*Add individual time tracking entries for detailed stage-specific analysis. Add the Card Name from Trello, the board it's from and any tags attached to the card. The Card Name is a required field.*")

        # Check if form should be cleared
        clear_form = st.session_state.get('clear_form', False)
        if clear_form:
            # Define all form field keys that need to be cleared
            form_keys_to_clear = [
                "manual_card_name",
                "manual_board_name",
                "manual_tag_select",
                "manual_add_new_tag",
                "manual_new_tag",
                # Time tracking field keys
                "user_editorial_r&d",
                "time_editorial_r&d",
                "user_editorial_writing",
                "time_editorial_writing",
                "user_1st_edit",
                "time_1st_edit",
                "user_2nd_edit",
                "time_2nd_edit",
                "user_design_r&d",
                "time_design_r&d",
                "user_in_design",
                "time_in_design",
                "user_1st_proof",
                "time_1st_proof",
                "user_2nd_proof",
                "time_2nd_proof",
                "user_editorial_sign_off",
                "time_editorial_sign_off",
                "user_design_sign_off",
                "time_design_sign_off",
            ]

            # Clear all form field keys from session state
            for key in form_keys_to_clear:
                if key in st.session_state:
                    del st.session_state[key]

            # Clear the flag
            del st.session_state['clear_form']

        # General fields
        col1, col2 = st.columns(2)
        with col1:
            card_name = st.text_input(
                "Card Name", placeholder="Enter book title", key="manual_card_name", value="" if clear_form else None
            )
        with col2:
            board_options = [
                "Accessible Readers",
                "Decodable Readers",
                "Freedom Readers",
                "Graphic Readers",
                "Non-Fiction",
                "Rapid Readers (Hi-Lo)",
            ]
            board_name = st.selectbox(
                "Board", options=board_options, key="manual_board_name", index=0 if clear_form else None
            )

        # Tag field - Multi-select
        existing_tags = get_tags_from_database(engine)

        # Create tag input - allow selecting multiple existing or adding new
        col1, col2 = st.columns([3, 1])
        with col1:
            selected_tags = st.multiselect(
                "Tags (optional)", existing_tags, key="manual_tag_select", placeholder="Choose an option"
            )
        with col2:
            add_new_tag = st.checkbox("Add New", key="manual_add_new_tag", value=False if clear_form else None)

        # If user wants to add new tag, show text input
        if add_new_tag:
            new_tag = st.text_input(
                "New Tag", placeholder="Enter new tag name", key="manual_new_tag", value="" if clear_form else None
            )
            if new_tag and new_tag.strip():
                new_tag_clean = new_tag.strip()
                if new_tag_clean not in selected_tags:
                    selected_tags.append(new_tag_clean)

        # Join multiple tags with commas for storage
        final_tag = ", ".join(selected_tags) if selected_tags else None

        st.subheader("Task Assignment & Estimates")
        st.markdown(
            "*Assign users to stages and set time estimates. You don't need to assign a user; that can be done later. Time should be added in hh:mm or decimal format. E.g. 1 hour and 30 minutes can be expressed as 1:30, 01:30 or 1.5.*"
        )

        # Define user groups for different types of work (alphabetically ordered)
        editorial_users = [
            "Not set",
            "Bethany Latham",
            "Charis Mather",
            "Noah Leatherland",
        ]
        design_users = [
            "Not set",
            "Amelia Harris",
            "Amy Li",
            "Drue Rintoul",
            "Jasmine Pointer",
            "Ker Ker Lee",
            "Rob Delph",
        ]

        # Time tracking fields with specific user groups
        time_fields = [
            ("Editorial R&D", "Editorial R&D", editorial_users),
            ("Editorial Writing", "Editorial Writing", editorial_users),
            ("1st Edit", "1st Edit", editorial_users),
            ("2nd Edit", "2nd Edit", editorial_users),
            ("Design R&D", "Design R&D", design_users),
            ("In Design", "In Design", design_users),
            ("1st Proof", "1st Proof", editorial_users),
            ("2nd Proof", "2nd Proof", editorial_users),
            ("Editorial Sign Off", "Editorial Sign Off", editorial_users),
            ("Design Sign Off", "Design Sign Off", design_users),
        ]

        # Calculate and display time estimations in real-time
        editorial_total = 0.0
        design_total = 0.0
        time_entries = {}

        editorial_fields = [
            "Editorial R&D",
            "Editorial Writing",
            "1st Edit",
            "2nd Edit",
            "1st Proof",
            "2nd Proof",
            "Editorial Sign Off",
        ]
        design_fields = ["Design R&D", "In Design", "Design Sign Off"]

        for field_label, list_name, user_options in time_fields:
            st.markdown(f"**{field_label} (hours)**")
            col1, col2 = st.columns([2, 1])

            with col1:
                selected_user = st.selectbox(
                    f"User for {field_label}",
                    user_options,
                    key=f"user_{list_name.replace(' ', '_').lower()}",
                    label_visibility="collapsed",
                )

            with col2:
                time_input = st.text_input(
                    f"Time for {field_label}",
                    key=f"time_{list_name.replace(' ', '_').lower()}",
                    label_visibility="collapsed",
                    placeholder="HH:MM or hours",
                )
                time_value = parse_hours_minutes(time_input)

            # Handle user selection and calculate totals
            # Allow time entries with or without user assignment
            if time_value and time_value > 0:
                final_user = selected_user if selected_user != "Not set" else "Not set"

                # Store the entry (user can be None for unassigned tasks)
                time_entries[list_name] = {'user': final_user, 'time_hours': time_value}

                # Add to category totals
                if list_name in editorial_fields:
                    editorial_total += time_value
                elif list_name in design_fields:
                    design_total += time_value

        total_estimation = editorial_total + design_total

        # Display real-time calculations
        st.markdown("---")
        st.markdown("**Time Estimations:**")
        st.write(f"Editorial Time Estimation: {editorial_total:.1f} hours")
        st.write(f"Design Time Estimation: {design_total:.1f} hours")
        st.write(f"**Total Time Estimation: {total_estimation:.1f} hours**")
        st.markdown("---")

        st.markdown("---")

        # Submit button outside of form
        if st.button("Add Entry", type="primary", key="manual_submit"):
            if not card_name:
                st.error("Please fill in Card Name field")
            else:
                try:
                    entries_added = 0
                    current_time = datetime.now(BST)

                    # Always create a book record first
                    create_book_record(engine, card_name, board_name, final_tag)

                    with engine.connect() as conn:
                        # Add estimate entries (task assignments with 0 time spent) if any exist
                        for list_name, entry_data in time_entries.items():
                            # Create task entry with 0 time spent - users will use timer to track actual time
                            # The time_hours value from the form is just for estimation display, not actual time spent

                            # Convert hours to seconds for estimate
                            estimate_seconds = int(entry_data['time_hours'] * 3600)

                            # Insert into database with 0 time spent but store the estimate
                            conn.execute(
                                text(
                                    '''
                                INSERT INTO trello_time_tracking
                                (card_name, user_name, list_name, time_spent_seconds, card_estimate_seconds, board_name, created_at, session_start_time, tag)
                                VALUES (:card_name, :user_name, :list_name, :time_spent_seconds, :card_estimate_seconds, :board_name, :created_at, :session_start_time, :tag)
                            '''
                                ),
                                {
                                    'card_name': card_name,
                                    'user_name': entry_data['user'],
                                    'list_name': list_name,
                                    'time_spent_seconds': 0,  # Start with 0 time spent
                                    'card_estimate_seconds': estimate_seconds,  # Store the estimate
                                    'board_name': board_name if board_name else None,
                                    'created_at': current_time,
                                    'session_start_time': None,  # No active session for manual entries
                                    'tag': final_tag,
                                },
                            )
                            entries_added += 1

                        conn.commit()

                    # Keep user on the Add Book tab

                    if entries_added > 0:
                        # Store success message in session state for permanent display
                        st.session_state.book_created_message = (
                            f"Book '{card_name}' created successfully with {entries_added} time estimates!"
                        )
                    else:
                        # Book created without tasks
                        st.session_state.book_created_message = f"Book '{card_name}' created successfully! You can add tasks later from the Book Progress tab."

                    # Set flag to clear form on next render instead of modifying session state directly
                    st.session_state.clear_form = True

                except Exception as e:
                    st.error(f"Error adding manual entry: {str(e)}")

        # Show permanent success message if book was created (below the button)
        if 'book_created_message' in st.session_state:
            st.success(st.session_state.book_created_message)

    with book_progress_tab:
        # Header with hover clipboard functionality
        st.markdown(
            """
        <div style="position: relative; display: inline-block;">
            <h1 style="display: inline-block; margin: 0;" id="book-completion-progress">Book Completion Progress</h1>
            <span class="header-copy-icon" style="
                opacity: 0;
                transition: opacity 0.2s;
                margin-left: 10px;
                cursor: pointer;
                color: #666;
                font-size: 20px;
                vertical-align: middle;
            " onclick="copyHeaderLink()">🔗</span>
        </div>
        <style>
        #book-completion-progress:hover + .header-copy-icon,
        .header-copy-icon:hover {
            opacity: 1;
        }
        </style>
        <script>
        function copyHeaderLink() {
            const url = window.location.origin + window.location.pathname + '#book-completion-progress';
            navigator.clipboard.writeText(url).then(function() {
                console.log('Copied header link to clipboard');
            });
        }
        </script>
        """,
            unsafe_allow_html=True,
        )
        st.markdown("Visual progress tracking for all books with individual task timers.")


        # Check if we have data from database with SSL connection retry
        total_records = 0
        max_retries = 3
        for attempt in range(max_retries):
            try:
                with engine.connect() as conn:
                    result = conn.execute(text("SELECT COUNT(*) FROM trello_time_tracking"))
                    total_records = result.scalar()
                    break  # Success, exit retry loop
            except Exception as e:
                if attempt < max_retries - 1:
                    # Try to recreate engine connection
                    time.sleep(0.5)  # Brief pause before retry
                    continue
                else:
                    # Final attempt failed, show error but continue
                    st.error(f"Database connection issue (attempt {attempt + 1}): {str(e)[:100]}...")
                    total_records = 0
                    break

        try:
            # Clear pending refresh state at start of render
            if 'pending_refresh' in st.session_state:
                del st.session_state.pending_refresh

            # Initialize variables to avoid UnboundLocalError
            df_from_db = None
            all_books = []

            if total_records and total_records > 0:

                # Get all books including those without tasks
                all_books = get_all_books(engine)

                # Get task data from database for book completion (exclude archived)
                df_from_db = pd.read_sql(
                    '''SELECT card_name as "Card name",
                       COALESCE(user_name, 'Not set') as "User",
                       list_name as "List",
                       time_spent_seconds as "Time spent (s)",
                       date_started as "Date started (f)",
                       card_estimate_seconds as "Card estimate(s)",
                       board_name as "Board", created_at, tag as "Tag"
                       FROM trello_time_tracking WHERE archived = FALSE ORDER BY created_at DESC''',
                    engine,
                )

                if not df_from_db.empty:
                    # Calculate total books for search title
                    books_with_tasks = set(df_from_db['Card name'].unique()) if not df_from_db.empty else set()
                    books_without_tasks = set(book[0] for book in all_books if book[0] not in books_with_tasks)
                    total_books = len(books_with_tasks | books_without_tasks)

                    # Add search bar only
                    search_query = st.text_input(
                        f"Search books by title ({total_books}):",
                        placeholder="Enter book title to search...",
                        key="completion_search",
                    )

                    # Initialize filtered_df
                    filtered_df = df_from_db.copy()

                    # Determine books to display
                    if search_query:
                        # Filter books based on search
                        import re

                        escaped_query = re.escape(search_query)
                        mask = filtered_df['Card name'].str.contains(escaped_query, case=False, na=False)
                        filtered_df = filtered_df[mask]

                        # Get unique books from both sources
                        books_with_tasks = set(filtered_df['Card name'].unique()) if not filtered_df.empty else set()
                        books_without_tasks = set(book[0] for book in all_books if book[0] not in books_with_tasks)

                        # Filter books without tasks based on search query
                        books_without_tasks = {
                            book for book in books_without_tasks if search_query.lower() in book.lower()
                        }

                        # Combine and sort
                        books_to_display = sorted(books_with_tasks | books_without_tasks)
                    else:
                        # Show all books by default
                        books_to_display = sorted(book[0] for book in all_books)

                    # Pagination setup
                    books_per_page = 10
                    if 'book_page' not in st.session_state:
                        st.session_state.book_page = 0

                    # Reset to first page if search changes
                    prev_search = st.session_state.get('prev_completion_search')
                    if search_query != prev_search:
                        st.session_state.book_page = 0
                    st.session_state.prev_completion_search = search_query

                    total_books_to_display = len(books_to_display)
                    start_idx = st.session_state.book_page * books_per_page
                    end_idx = start_idx + books_per_page
                    books_subset = books_to_display[start_idx:end_idx]

                    # Only display books if we have search results
                    if books_subset:
                        # Display each book with enhanced visualization
                        for book_title in books_subset:
                            # Check if book has tasks
                            if not filtered_df.empty:
                                book_mask = filtered_df['Card name'] == book_title
                                book_data = filtered_df[book_mask].copy()
                            else:
                                book_data = pd.DataFrame()

                            # Debug: Let's see what we have
                            # st.write(f"DEBUG: Book '{book_title}' - book_data shape: {book_data.shape}")
                            # if not book_data.empty:
                            #     st.write(f"DEBUG: Book tasks found: {book_data['List'].unique()}")
                            # else:
                            #     st.write(f"DEBUG: Book data is empty for '{book_title}'")

                            # If book has no tasks, create empty data structure
                            if book_data.empty:
                                # Get book info from all_books
                                book_info = next((book for book in all_books if book[0] == book_title), None)
                                if book_info:
                                    # Create minimal book data structure
                                    book_data = pd.DataFrame(
                                        {
                                            'Card name': [book_title],
                                            'User': ['Not set'],
                                            'List': ['No tasks assigned'],
                                            'Time spent (s)': [0],
                                            'Date started (f)': [None],
                                            'Card estimate(s)': [0],
                                            'Board': [book_info[1] if book_info[1] else 'Not set'],
                                            'Tag': [book_info[2] if book_info[2] else None],
                                        }
                                    )

                            # Calculate overall progress using stage-based estimates
                            total_time_spent = book_data['Time spent (s)'].sum()

                            # Calculate total estimated time from the database entries
                            # Sum up all estimates stored in the database for this book
                            estimated_time = 0
                            if 'Card estimate(s)' in book_data.columns:
                                book_estimates = book_data['Card estimate(s)'].fillna(0).sum()
                                if book_estimates > 0:
                                    estimated_time = book_estimates

                            # If no estimates in database, use reasonable defaults per stage
                            if estimated_time == 0:
                                default_stage_estimates = {
                                    'Editorial R&D': 2 * 3600,  # 2 hours default
                                    'Editorial Writing': 8 * 3600,  # 8 hours default
                                    '1st Edit': 4 * 3600,  # 4 hours default
                                    '2nd Edit': 2 * 3600,  # 2 hours default
                                    'Design R&D': 3 * 3600,  # 3 hours default
                                    'In Design': 6 * 3600,  # 6 hours default
                                    '1st Proof': 2 * 3600,  # 2 hours default
                                    '2nd Proof': 1.5 * 3600,  # 1.5 hours default
                                    'Editorial Sign Off': 0.5 * 3600,  # 30 minutes default
                                    'Design Sign Off': 0.5 * 3600,  # 30 minutes default
                                }
                                unique_stages = book_data['List'].unique()
                                estimated_time = sum(
                                    default_stage_estimates.get(stage, 3600) for stage in unique_stages
                                )

                            # Calculate completion percentage for display
                            if estimated_time > 0:
                                completion_percentage = (total_time_spent / estimated_time) * 100
                                progress_text = f"{format_seconds_to_time(total_time_spent)}/{format_seconds_to_time(estimated_time)} ({completion_percentage:.1f}%)"
                            else:
                                completion_percentage = 0
                                progress_text = f"Total: {format_seconds_to_time(total_time_spent)} (No estimate)"

                            # Check for active timers more efficiently
                            has_active_timer = any(
                                timer_key.startswith(f"{book_title}_") and active
                                for timer_key, active in st.session_state.timers.items()
                            )

                            # Check if all tasks are completed (only if book has tasks)
                            all_tasks_completed = False
                            completion_emoji = ""
                            if not book_data.empty and book_data['List'].iloc[0] != 'No tasks assigned':
                                # Check completion status from database
                                all_tasks_completed = check_all_tasks_completed(engine, book_title)
                                completion_emoji = "✅ " if all_tasks_completed else ""

                            # Create book title with progress percentage
                            if estimated_time > 0:
                                if completion_percentage > 100:
                                    over_percentage = completion_percentage - 100
                                    book_title_with_progress = (
                                        f"{completion_emoji}**{book_title}** ({over_percentage:.1f}% over estimate)"
                                    )
                                else:
                                    book_title_with_progress = (
                                        f"{completion_emoji}**{book_title}** ({completion_percentage:.1f}%)"
                                    )
                            else:
                                book_title_with_progress = f"{completion_emoji}**{book_title}** (No estimate)"

                            # Check if book should be expanded (either has active timer or was manually expanded)
                            expanded_key = f"expanded_{book_title}"
                            if expanded_key not in st.session_state:
                                st.session_state[expanded_key] = has_active_timer

                            with st.expander(book_title_with_progress, expanded=st.session_state[expanded_key]):
                                # Show progress bar and completion info at the top
                                progress_bar_html = f"""
                                    <div style="width: 50%; background-color: #f0f0f0; border-radius: 5px; height: 10px; margin: 8px 0;">
                                    <div style="width: {min(completion_percentage, 100):.1f}%; background-color: #2AA395; height: 100%; border-radius: 5px;"></div>
                                    </div>
                                    """
                                st.markdown(progress_bar_html, unsafe_allow_html=True)
                                st.markdown(
                                    f'<div style="font-size: 14px; color: #666; margin-bottom: 10px;">{progress_text}</div>',
                                    unsafe_allow_html=True,
                                )

                                # Display tag if available
                                book_tags = book_data['Tag'].dropna().unique()
                                if len(book_tags) > 0 and book_tags[0]:
                                    # Handle multiple tags (comma-separated)
                                    tag_display = book_tags[0]
                                    # If there are commas, it means multiple tags
                                    if ',' in tag_display:
                                        tag_display = tag_display.replace(',', ', ')  # Ensure proper spacing
                                    st.markdown(
                                        f'<div style="font-size: 14px; color: #888; margin-bottom: 10px;"><strong>Tags:</strong> {tag_display}</div>',
                                        unsafe_allow_html=True,
                                    )

                                st.markdown("---")

                                # Define the order of stages to match the actual data entry form
                                stage_order = [
                                    'Editorial R&D',
                                    'Editorial Writing',
                                    '1st Edit',
                                    '2nd Edit',
                                    'Design R&D',
                                    'In Design',
                                    '1st Proof',
                                    '2nd Proof',
                                    'Editorial Sign Off',
                                    'Design Sign Off',
                                ]

                                # Group by stage/list and aggregate by user
                                stages_grouped = book_data.groupby('List')

                                # Display stages in accordion style (each stage as its own expander)
                                stage_counter = 0
                                for stage_name in stage_order:
                                    if stage_name in stages_grouped.groups:
                                        stage_data = stages_grouped.get_group(stage_name)

                                        # Check if this stage has any active timers (efficient lookup)
                                        stage_has_active_timer = any(
                                            timer_key.startswith(f"{book_title}_{stage_name}_") and active
                                            for timer_key, active in st.session_state.timers.items()
                                        )

                                        # Aggregate time by user for this stage
                                        user_aggregated = (
                                            stage_data.groupby('User')['Time spent (s)'].sum().reset_index()
                                        )

                                        # Create a summary for the expander title showing all users and their progress
                                        stage_summary_parts = []
                                        summary_users = set()
                                        for idx, user_task in user_aggregated.iterrows():
                                            user_name = user_task['User']
                                            if user_name in summary_users:
                                                continue
                                            summary_users.add(user_name)
                                            actual_time = user_task['Time spent (s)']

                                            # Get estimated time from the database for this specific user/stage combination
                                            user_stage_data = stage_data[stage_data['User'] == user_name]
                                            estimated_time_for_user = 3600  # Default 1 hour

                                            if (
                                                not user_stage_data.empty
                                                and 'Card estimate(s)' in user_stage_data.columns
                                            ):
                                                # Find the first record that has a non-null, non-zero estimate
                                                estimates = user_stage_data['Card estimate(s)'].dropna()
                                                non_zero_estimates = estimates[estimates > 0]
                                                if not non_zero_estimates.empty:
                                                    estimated_time_for_user = non_zero_estimates.iloc[0]

                                            # Check if task is completed and add tick emoji
                                            task_completed = get_task_completion(
                                                engine, book_title, user_name, stage_name
                                            )
                                            completion_emoji = "✅ " if task_completed else ""

                                            # Format times for display
                                            actual_time_str = format_seconds_to_time(actual_time)
                                            estimated_time_str = format_seconds_to_time(estimated_time_for_user)
                                            user_display = (
                                                user_name if user_name and user_name != "Not set" else "Unassigned"
                                            )

                                            stage_summary_parts.append(
                                                f"{user_display} | {actual_time_str}/{estimated_time_str} {completion_emoji}".rstrip()
                                            )

                                        # Create expander title with stage name and user summaries
                                        if stage_summary_parts:
                                            expander_title = f"**{stage_name}** | " + " | ".join(stage_summary_parts)
                                        else:
                                            expander_title = stage_name

                                        # Check if stage should be expanded (either has active timer or was manually expanded)
                                        stage_expanded_key = f"stage_expanded_{book_title}_{stage_name}"
                                        if stage_expanded_key not in st.session_state:
                                            st.session_state[stage_expanded_key] = stage_has_active_timer

                                        with st.expander(expander_title, expanded=st.session_state[stage_expanded_key]):
                                            processed_tasks = set()
                                            # Show one task per user for this stage
                                            for idx, user_task in user_aggregated.iterrows():
                                                user_name = user_task['User']
                                                task_key = f"{book_title}_{stage_name}_{user_name}"
                                                if task_key in processed_tasks:
                                                    continue
                                                processed_tasks.add(task_key)
                                                actual_time = user_task['Time spent (s)']
                                                task_key = f"{book_title}_{stage_name}_{user_name}"
                                                session_id = st.session_state.get('timer_session_counts', {}).get(task_key, 0)

                                                # Get estimated time from the database for this specific user/stage combination
                                                user_stage_data = stage_data[stage_data['User'] == user_name]
                                                estimated_time_for_user = 3600  # Default 1 hour

                                                if (
                                                    not user_stage_data.empty
                                                    and 'Card estimate(s)' in user_stage_data.columns
                                                ):
                                                    # Find the first record that has a non-null, non-zero estimate
                                                    estimates = user_stage_data['Card estimate(s)'].dropna()
                                                    non_zero_estimates = estimates[estimates > 0]
                                                    if not non_zero_estimates.empty:
                                                        estimated_time_for_user = non_zero_estimates.iloc[0]

                                                # Create columns for task info and timer
                                                col1, col2, col3 = st.columns([4, 1, 3])

                                                with col1:
                                                    # User assignment dropdown
                                                    current_user = user_name if user_name else "Not set"

                                                    # Determine user options based on stage type
                                                    if stage_name in [
                                                        "Editorial R&D",
                                                        "Editorial Writing",
                                                        "1st Edit",
                                                        "2nd Edit",
                                                        "1st Proof",
                                                        "2nd Proof",
                                                        "Editorial Sign Off",
                                                    ]:
                                                        user_options = [
                                                            "Not set",
                                                            "Bethany Latham",
                                                            "Charis Mather",
                                                            "Noah Leatherland",
                                                        ]
                                                    else:  # Design stages
                                                        user_options = [
                                                            "Not set",
                                                            "Amelia Harris",
                                                            "Amy Li",
                                                            "Drue Rintoul",
                                                            "Jasmine Pointer",
                                                            "Ker Ker Lee",
                                                            "Rob Delph",
                                                        ]

                                                    # Find current user index
                                                    try:
                                                        current_index = user_options.index(current_user)
                                                    except ValueError:
                                                        current_index = 0  # Default to "Not set"

                                                    import hashlib

                                                    def stable_hash(*values) -> str:
                                                        s = "||".join("" if v is None else str(v) for v in values)
                                                        return hashlib.md5(s.encode()).hexdigest()[:8]

                                                    session_id = stable_hash(book_title, stage_name, user_name, str(idx))
                                                    key_prefix = f"reassign_{session_id}"
                                                    st.button("Reassign", key=f"{key_prefix}_btn")
                                                    st.text_input("Notes", key=f"{key_prefix}_notes")


                                                    key_prefix = f"reassign_{session_id}"
                                                    st.button("Reassign", key=f"{key_prefix}_btn")
                                                    st.text_input("Notes", key=f"{key_prefix}_notes")


                                                    # Display progress information directly under user dropdown
                                                    if user_name and user_name != "Not set":
                                                        # Use the actual_time variable that's already calculated for this user/stage
                                                        if estimated_time_for_user and estimated_time_for_user > 0:
                                                            progress_percentage = actual_time / estimated_time_for_user
                                                            time_spent_formatted = format_seconds_to_time(actual_time)
                                                            estimated_formatted = format_seconds_to_time(
                                                                estimated_time_for_user
                                                            )

                                                            # Progress bar
                                                            progress_value = max(0.0, min(progress_percentage, 1.0))
                                                            st.progress(progress_value)

                                                            # Progress text
                                                            if progress_percentage > 1.0:
                                                                st.write(
                                                                    f"{(progress_percentage - 1) * 100:.1f}% over estimate"
                                                                )
                                                            elif progress_percentage == 1.0:
                                                                st.write("COMPLETE: 100%")
                                                            else:
                                                                st.write(f"{progress_percentage * 100:.1f}% complete")

                                                            # Time information
                                                            st.write(
                                                                f"Time: {time_spent_formatted} / {estimated_formatted}"
                                                            )

                                                            # Completion checkbox - always get fresh status from database
                                                            completion_key = (
                                                                f"complete_{book_title}_{stage_name}_{user_name}"
                                                            )
                                                            current_completion_status = get_task_completion(
                                                                engine, book_title, user_name, stage_name
                                                            )

                                                            # Update session state with database value
                                                            st.session_state[completion_key] = current_completion_status

                                                            new_completion_status = st.checkbox(
                                                                "Completed",
                                                                value=current_completion_status,
                                                                key=f"checkbox_{completion_key}",
                                                            )

                                                            # Update completion status if changed
                                                            if new_completion_status != current_completion_status:
                                                                update_task_completion(
                                                                    engine,
                                                                    book_title,
                                                                    user_name,
                                                                    stage_name,
                                                                    new_completion_status,
                                                                )
                                                                # Update session state immediately
                                                                st.session_state[completion_key] = new_completion_status

                                                                # Clear any cached completion status to force refresh
                                                                completion_cache_key = f"book_completion_{book_title}"
                                                                if completion_cache_key in st.session_state:
                                                                    del st.session_state[completion_cache_key]

                                                                # Store success message for display without immediate refresh
                                                                success_msg_key = f"completion_success_{task_key}"
                                                                status_text = (
                                                                    "✅ Marked as completed"
                                                                    if new_completion_status
                                                                    else "❌ Marked as incomplete"
                                                                )
                                                                st.session_state[success_msg_key] = status_text

                                                                # Set flag for book-level completion update
                                                                st.session_state['completion_changed'] = True
                                                        else:
                                                            st.write("No time estimate set")

                                                    # Handle user reassignment with improved state management
                                                    if new_user != current_user:
                                                        try:
                                                            with engine.connect() as conn:
                                                                # Update user assignment in database
                                                                new_user_value = (
                                                                    new_user if new_user != "Not set" else None
                                                                )
                                                                old_user_value = (
                                                                    user_name if user_name != "Not set" else None
                                                                )

                                                                conn.execute(
                                                                    text(
                                                                        '''
                                                                        UPDATE trello_time_tracking
                                                                        SET user_name = :new_user
                                                                        WHERE card_name = :card_name
                                                                        AND list_name = :list_name
                                                                        AND COALESCE(user_name, '') = COALESCE(:old_user, '')
                                                                    '''
                                                                    ),
                                                                    {
                                                                        'new_user': new_user_value,
                                                                        'card_name': book_title,
                                                                        'list_name': stage_name,
                                                                        'old_user': old_user_value,
                                                                    },
                                                                )
                                                                conn.commit()

                                                                # Clear relevant session state to force refresh
                                                                keys_to_clear = [
                                                                    k
                                                                    for k in st.session_state.keys()
                                                                    if book_title in k and stage_name in k
                                                                ]
                                                                for key in keys_to_clear:
                                                                    if key.startswith(('complete_', 'timer_')):
                                                                        del st.session_state[key]

                                                                # Store success message instead of immediate refresh
                                                                success_key = f"reassign_success_{book_title}_{stage_name}_{user_name}_{session_id}_{idx}"
                                                                st.session_state[success_key] = (
                                                                    f"User reassigned from {current_user} to {new_user}"
                                                                )

                                                                # User reassignment completed
                                                        except Exception as e:
                                                            st.error(f"Error reassigning user: {str(e)}")

                                        with col2:
                                            # Empty space - timer moved to button column
                                            st.write("")

                                        with col3:
                                            # Start/Stop timer button with timer display
                                            if task_key not in st.session_state.timers:
                                                st.session_state.timers[task_key] = False

                                            # Timer controls and display
                                            if st.session_state.timers[task_key]:
                                                # Timer is active - show simple stop control
                                                if task_key in st.session_state.timer_start_times:

                                                    # Simple timer calculation
                                                    start_time = st.session_state.timer_start_times[task_key]
                                                    accumulated = st.session_state.timer_accumulated_time.get(task_key, 0)
                                                    paused = st.session_state.timer_paused.get(task_key, False)

                                                    current_elapsed = 0 if paused else calculate_timer_elapsed_time(start_time)
                                                    elapsed_seconds = accumulated + current_elapsed
                                                    elapsed_str = format_seconds_to_time(elapsed_seconds)

                                                    # Display recording status with a client-side timer
                                                    status_label = "Paused" if paused else "Recording"
                                                    timer_id = f"timer_{task_key}_{session_id}"
                                                    components.html(
                                                        render_basic_js_timer(
                                                            timer_id,
                                                            status_label,
                                                            elapsed_seconds,
                                                            paused,
                                                        ),
                                                        height=40,

                                                    )

                                                    # Second row with pause and stop controls
                                                    timer_row2_col1, timer_row2_col2 = st.columns([1.5, 1])

                                                    with timer_row2_col1:
                                                        pause_label = "Resume" if paused else "Pause"

                                                        if st.button(
                                                            pause_label,
                                                            key=f"pause_{task_key}_{session_id}",
                                                        ):
                                                            if paused:
                                                                resume_time = datetime.utcnow().replace(tzinfo=timezone.utc).astimezone(BST)
                                                                st.session_state.timer_start_times[task_key] = resume_time
                                                                st.session_state.timer_paused[task_key] = False
                                                                update_active_timer_state(
                                                                    engine,
                                                                    task_key,
                                                                    accumulated,
                                                                    False,
                                                                    resume_time,
                                                                )
                                                            else:
                                                                elapsed_since_start = calculate_timer_elapsed_time(start_time)
                                                                new_accum = accumulated + elapsed_since_start
                                                                st.session_state.timer_accumulated_time[task_key] = new_accum
                                                                st.session_state.timer_paused[task_key] = True
                                                                update_active_timer_state(
                                                                    engine,
                                                                    task_key,
                                                                    new_accum,
                                                                    True,
                                                                )
                                                            st.rerun()

                                                    with timer_row2_col2:
                                                        if st.button("Stop", key=f"stop_{task_key}_{session_id}"):
                                                            final_time = elapsed_seconds
                                                            stop_active_timer(engine, task_key)

                                                            # Keep expanded states
                                                            expanded_key = f"expanded_{book_title}"
                                                            st.session_state[expanded_key] = True
                                                            stage_expanded_key = (
                                                                f"stage_expanded_{book_title}_{stage_name}"
                                                            )
                                                            st.session_state[stage_expanded_key] = True

                                                            # Always clear timer states first to prevent double-processing
                                                            st.session_state.timers[task_key] = False
                                                            timer_start_time = st.session_state.timer_start_times.get(
                                                                task_key
                                                            )

                                                            # Save to database only if time > 0
                                                            if final_time > 0 and timer_start_time:
                                                                try:
                                                                    user_original_data = stage_data[
                                                                        stage_data['User'] == user_name
                                                                    ].iloc[0]
                                                                    board_name = user_original_data['Board']
                                                                    existing_tag = (
                                                                        user_original_data.get('Tag', None)
                                                                        if 'Tag' in user_original_data
                                                                        else None
                                                                    )

                                                                    with engine.connect() as conn:
                                                                        # Use ON CONFLICT to handle duplicate entries by updating existing records
                                                                        conn.execute(
                                                                            text(
                                                                                '''
                                            INSERT INTO trello_time_tracking
                                            (card_name, user_name, list_name, time_spent_seconds,
                                             date_started, session_start_time, board_name, tag)
                                            VALUES (:card_name, :user_name, :list_name, :time_spent_seconds,
                                                   :date_started, :session_start_time, :board_name, :tag)
                                            ON CONFLICT (card_name, user_name, list_name, date_started, time_spent_seconds)
                                            DO UPDATE SET
                                                session_start_time = EXCLUDED.session_start_time,
                                                board_name = EXCLUDED.board_name,
                                                tag = EXCLUDED.tag,
                                                created_at = CURRENT_TIMESTAMP
                                        '''
                                                                            ),
                                                                            {
                                                                                'card_name': book_title,
                                                                                'user_name': user_name,
                                                                                'list_name': stage_name,
                                                                                'time_spent_seconds': final_time,
                                                                                'date_started': timer_start_time.date(),
                                                                                'session_start_time': timer_start_time,
                                                                                'board_name': board_name,
                                                                                'tag': existing_tag,
                                                                            },
                                                                        )

                                                                        # Remove from active timers
                                                                        conn.execute(
                                                                            text(
                                                                                'DELETE FROM active_timers WHERE timer_key = :timer_key'
                                                                            ),
                                                                            {'timer_key': task_key},
                                                                        )
                                                                        conn.commit()

                                                                    # Store success message for display at bottom
                                                                    success_msg_key = f"timer_success_{task_key}"
                                                                    st.session_state[success_msg_key] = (
                                                                        f"Added {elapsed_str} to {book_title} - {stage_name}"
                                                                    )

                                                                    # Timer stopped successfully
                                                                except Exception as e:
                                                                    st.error(f"Error saving timer data: {str(e)}")
                                                                    # Still try to clean up active timer from database on error
                                                                    try:
                                                                        with engine.connect() as conn:
                                                                            conn.execute(
                                                                                text(
                                                                                    'DELETE FROM active_timers WHERE timer_key = :timer_key'
                                                                                ),
                                                                                {'timer_key': task_key},
                                                                            )
                                                                            conn.commit()
                                                                    except:
                                                                        pass  # Ignore cleanup errors
                                                            else:
                                                                # Even if no time to save, clean up active timer
                                                                try:
                                                                    with engine.connect() as conn:
                                                                        conn.execute(
                                                                            text(
                                                                                'DELETE FROM active_timers WHERE timer_key = :timer_key'
                                                                            ),
                                                                            {'timer_key': task_key},
                                                                        )
                                                                        conn.commit()
                                                                except:
                                                                    pass  # Ignore cleanup errors

                                                            # Clear timer states
                                                            st.session_state.setdefault('timer_session_counts', {})
                                                            st.session_state.timer_session_counts[task_key] = (
                                                                st.session_state.timer_session_counts.get(task_key, 0) + 1
                                                            )
                                                            if task_key in st.session_state.timer_start_times:
                                                                del st.session_state.timer_start_times[task_key]
                                                            if task_key in st.session_state.timer_accumulated_time:
                                                                del st.session_state.timer_accumulated_time[task_key]
                                                            if task_key in st.session_state.timer_paused:
                                                                del st.session_state.timer_paused[task_key]

                                                            # Refresh the interface so totals update immediately
                                                            st.rerun()

                                            else:
                                                # Timer is not active - show Start button
                                                if st.button("Start", key=f"start_{task_key}_{session_id}"):
                                                    # Preserve expanded state before rerun
                                                    expanded_key = f"expanded_{book_title}"
                                                    st.session_state[expanded_key] = True

                                                    # Also preserve stage expanded state
                                                    stage_expanded_key = f"stage_expanded_{book_title}_{stage_name}"
                                                    st.session_state[stage_expanded_key] = True

                                                    # Start timer - use UTC for consistency
                                                    start_time_utc = datetime.utcnow().replace(tzinfo=timezone.utc)
                                                    # Convert to BST for display/storage but keep UTC calculation base
                                                    start_time_bst = start_time_utc.astimezone(BST)
                                                    st.session_state.timers[task_key] = True
                                                    st.session_state.timer_start_times[task_key] = start_time_bst
                                                    st.session_state.timer_paused[task_key] = False
                                                    st.session_state.timer_accumulated_time[task_key] = 0

                                                    # Save to database for persistence
                                                    user_original_data = stage_data[
                                                        stage_data['User'] == user_name
                                                    ].iloc[0]
                                                    board_name = user_original_data['Board']

                                                    save_active_timer(
                                                        engine,
                                                        task_key,
                                                        book_title,
                                                        user_name if user_name != "Not set" else None,
                                                        stage_name,
                                                        board_name,
                                                        start_time_bst,
                                                        accumulated_seconds=0,
                                                        is_paused=False,
                                                    )

                                                    st.rerun()

                                            # Manual time entry section
                                            st.write("**Manual Entry:**")

                                            # Create a form to handle Enter key properly
                                            with st.form(key=f"time_form_{task_key}_{session_id}"):
                                                manual_time = st.text_input(
                                                    "Add time (hh:mm:ss):", placeholder="01:30:00"
                                                )

                                                # Hide the submit button and form styling with CSS
                                                st.markdown(
                                                    """
                                                    <style>
                                                    div[data-testid="stForm"] button {
                                                        display: none;
                                                    }
                                                    div[data-testid="stForm"] {
                                                        border: none !important;
                                                        background: none !important;
                                                        padding: 0 !important;
                                                    }
                                                    </style>
                                                    """,
                                                    unsafe_allow_html=True,
                                                )

                                                submitted = st.form_submit_button("Add Time")

                                                if submitted and manual_time:
                                                    try:
                                                        # Parse the time format hh:mm:ss
                                                        time_parts = manual_time.split(':')
                                                        if len(time_parts) == 3:
                                                            hours = int(time_parts[0])
                                                            minutes = int(time_parts[1])
                                                            seconds = int(time_parts[2])

                                                            # Validate individual components
                                                            if hours > 100:
                                                                st.error(
                                                                    f"Maximum hours allowed is 100. You entered {hours} hours."
                                                                )
                                                            elif minutes >= 60:
                                                                st.error(
                                                                    f"Minutes must be less than 60. You entered {minutes} minutes."
                                                                )
                                                            elif seconds >= 60:
                                                                st.error(
                                                                    f"Seconds must be less than 60. You entered {seconds} seconds."
                                                                )
                                                            else:
                                                                total_seconds = hours * 3600 + minutes * 60 + seconds

                                                                # Validate maximum time (100 hours = 360,000 seconds)
                                                                max_seconds = 100 * 3600  # 360,000 seconds
                                                                if total_seconds > max_seconds:
                                                                    st.error(
                                                                        f"Maximum time allowed is 100:00:00. You entered {manual_time}"
                                                                    )
                                                                elif total_seconds > 0:
                                                                    # Add manual time to database
                                                                    try:
                                                                        # Get board name from original data
                                                                        user_original_data = stage_data[
                                                                            stage_data['User'] == user_name
                                                                        ].iloc[0]
                                                                        board_name = user_original_data['Board']
                                                                        # Get existing tag from original data
                                                                        existing_tag = (
                                                                            user_original_data.get('Tag', None)
                                                                            if 'Tag' in user_original_data
                                                                            else None
                                                                        )

                                                                        # Get current completion status to preserve it
                                                                        completion_key = f"complete_{book_title}_{stage_name}_{user_name}"
                                                                        current_completion = get_task_completion(
                                                                            engine, book_title, user_name, stage_name
                                                                        )
                                                                        # Also check session state in case it was just changed
                                                                        if completion_key in st.session_state:
                                                                            current_completion = st.session_state[
                                                                                completion_key
                                                                            ]

                                                                        # Preserve expanded state before rerun
                                                                        expanded_key = f"expanded_{book_title}"
                                                                        st.session_state[expanded_key] = True

                                                                        # Preserve stage expanded state
                                                                        stage_expanded_key = (
                                                                            f"stage_expanded_{book_title}_{stage_name}"
                                                                        )
                                                                        st.session_state[stage_expanded_key] = True

                                                                        with engine.connect() as conn:
                                                                            conn.execute(
                                                                                text(
                                                                                    '''
                                                                                    INSERT INTO trello_time_tracking
                                                                                    (card_name, user_name, list_name, time_spent_seconds, board_name, created_at, tag, completed)
                                                                                    VALUES (:card_name, :user_name, :list_name, :time_spent_seconds, :board_name, :created_at, :tag, :completed)
                                                                                '''
                                                                                ),
                                                                                {
                                                                                    'card_name': book_title,
                                                                                    'user_name': user_name,
                                                                                    'list_name': stage_name,
                                                                                    'time_spent_seconds': total_seconds,
                                                                                    'board_name': board_name,
                                                                                    'created_at': datetime.now(BST),
                                                                                    'tag': existing_tag,
                                                                                    'completed': current_completion,
                                                                                },
                                                                            )
                                                                            conn.commit()

                                                                        # Store success message in session state for display
                                                                        success_msg_key = (
                                                                            f"manual_time_success_{task_key}"
                                                                        )
                                                                        st.session_state[success_msg_key] = (
                                                                            f"Added {manual_time} to progress"
                                                                        )

                                                                    except Exception as e:
                                                                        st.error(f"Error saving time: {str(e)}")
                                                                else:
                                                                    st.error("Time must be greater than 00:00:00")
                                                        else:
                                                            st.error("Please use format hh:mm:ss (e.g., 01:30:00)")
                                                    except ValueError:
                                                        st.error("Please enter valid numbers in hh:mm:ss format")

                                            # Display various success messages
                                            # Timer success message
                                            timer_success_key = f"timer_success_{task_key}"
                                            if timer_success_key in st.session_state:
                                                st.success(st.session_state[timer_success_key])
                                                del st.session_state[timer_success_key]

                                            # Manual time success message
                                            manual_success_key = f"manual_time_success_{task_key}"
                                            if manual_success_key in st.session_state:
                                                st.success(st.session_state[manual_success_key])
                                                del st.session_state[manual_success_key]

                                            # Completion status success message
                                            completion_success_key = f"completion_success_{task_key}"
                                            if completion_success_key in st.session_state:
                                                st.success(st.session_state[completion_success_key])
                                                del st.session_state[completion_success_key]

                                            # User reassignment success message
                                            reassign_success_key = f"reassign_success_{book_title}_{stage_name}_{user_name}_{session_id}_{idx}"
                                            if reassign_success_key in st.session_state:
                                                st.success(st.session_state[reassign_success_key])
                                                del st.session_state[reassign_success_key]

                                # Show count of running timers (refresh buttons now appear under individual timers)
                                running_timers = [
                                    k for k, v in st.session_state.timers.items() if v and book_title in k
                                ]
                                if running_timers:
                                    st.write(f"{len(running_timers)} timer(s) running")

                                # Add stage dropdown
                                available_stages = get_available_stages_for_book(engine, book_title)
                                if available_stages:
                                    st.markdown("---")
                                    col1, col2 = st.columns([3, 1])

                                    with col1:
                                        selected_stage = st.selectbox(
                                            "Add stage:",
                                            options=["Select a stage to add..."] + available_stages,
                                            key=f"add_stage_{book_title}",
                                        )

                                    with col2:
                                        time_estimate = st.number_input(
                                            "Hours:",
                                            min_value=0.0,
                                            step=0.1,
                                            format="%.1f",
                                            value=1.0,
                                            key=f"add_stage_time_{book_title}",
                                            on_change=None,  # Prevent automatic refresh
                                        )

                                    if selected_stage != "Select a stage to add...":
                                        # Get the current time estimate from session state
                                        time_estimate_key = f"add_stage_time_{book_title}"
                                        current_time_estimate = st.session_state.get(time_estimate_key, 1.0)

                                        # Get book info for board name and tag
                                        book_info = next((book for book in all_books if book[0] == book_title), None)
                                        board_name = book_info[1] if book_info else None
                                        tag = book_info[2] if book_info else None

                                        # Convert hours to seconds for estimate
                                        estimate_seconds = int(current_time_estimate * 3600)

                                        if add_stage_to_book(
                                            engine, book_title, selected_stage, board_name, tag, estimate_seconds
                                        ):
                                            st.success(
                                                f"Added {selected_stage} to {book_title} with {current_time_estimate} hour estimate"
                                            )
                                            # Stage added successfully
                                        else:
                                            st.error("Failed to add stage")

                                # Remove stage section at the bottom left of each book
                                if stages_grouped.groups:  # Only show if book has stages
                                    st.markdown("---")
                                    remove_col1, remove_col2, remove_col3 = st.columns([2, 1, 1])

                                    with remove_col1:
                                        # Get all current stages for this book
                                        current_stages_with_users = []
                                        for stage_name in stage_order:
                                            if stage_name in stages_grouped.groups:
                                                stage_data = stages_grouped.get_group(stage_name)
                                                user_aggregated = (
                                                    stage_data.groupby('User')['Time spent (s)'].sum().reset_index()
                                                )
                                                for idx, user_task in user_aggregated.iterrows():
                                                    user_name = user_task['User']
                                                    user_display = (
                                                        user_name
                                                        if user_name and user_name != "Not set"
                                                        else "Unassigned"
                                                    )
                                                    current_stages_with_users.append(f"{stage_name} ({user_display})")

                                        if current_stages_with_users:
                                            selected_remove_stage = st.selectbox(
                                                "Remove stage:",
                                                options=["Select stage to remove..."] + current_stages_with_users,
                                                key=f"remove_stage_select_{book_title}",
                                            )

                                            if selected_remove_stage != "Select stage to remove...":
                                                # Parse the selection to get stage name and user
                                                stage_user_match = selected_remove_stage.split(" (")
                                                remove_stage_name = stage_user_match[0]
                                                remove_user_name = stage_user_match[1].rstrip(")")
                                                if remove_user_name == "Unassigned":
                                                    remove_user_name = "Not set"

                                                if st.button(
                                                    "Remove",
                                                    key=f"remove_confirm_{book_title}_{remove_stage_name}_{remove_user_name}",
                                                    type="secondary",
                                                ):
                                                    if delete_task_stage(
                                                        engine, book_title, remove_user_name, remove_stage_name
                                                    ):
                                                        st.success(
                                                            f"Removed {remove_stage_name} for {remove_user_name}"
                                                        )
                                                        # Manual time added successfully
                                                    else:
                                                        st.error("Failed to remove stage")

                                # Archive and Delete buttons at the bottom of each book
                                st.markdown("---")
                                col1, col2 = st.columns(2)

                                with col1:
                                    if st.button(
                                        f"Archive '{book_title}'",
                                        key=f"archive_{book_title}",
                                        help="Move this book to archive",
                                    ):
                                        try:
                                            with engine.connect() as conn:
                                                # Check if book has time tracking records
                                                result = conn.execute(
                                                    text(
                                                        '''
                                                        SELECT COUNT(*) FROM trello_time_tracking
                                                        WHERE card_name = :card_name
                                                    '''
                                                    ),
                                                    {'card_name': book_title},
                                                )
                                                record_count = result.scalar()

                                                if record_count > 0:
                                                    # Archive existing time tracking records
                                                    conn.execute(
                                                        text(
                                                            '''
                                                            UPDATE trello_time_tracking
                                                            SET archived = TRUE
                                                            WHERE card_name = :card_name
                                                        '''
                                                        ),
                                                        {'card_name': book_title},
                                                    )
                                                else:
                                                    # Create a placeholder archived record for books without tasks
                                                    conn.execute(
                                                        text(
                                                            '''
                                                            INSERT INTO trello_time_tracking
                                                            (card_name, user_name, list_name, time_spent_seconds,
                                                             card_estimate_seconds, board_name, archived, created_at)
                                                            VALUES (:card_name, 'Not set', 'No tasks assigned', 0,
                                                                   0, 'Manual Entry', TRUE, NOW())
                                                        '''
                                                        ),
                                                        {'card_name': book_title},
                                                    )

                                                # Archive the book in books table
                                                conn.execute(
                                                    text(
                                                        '''
                                                        UPDATE books
                                                        SET archived = TRUE
                                                        WHERE card_name = :book_name
                                                    '''
                                                    ),
                                                    {'book_name': book_title},
                                                )

                                                conn.commit()

                                            # Keep user on the current tab
                                            st.success(f"'{book_title}' has been archived successfully!")
                                            # Archive operation completed
                                        except Exception as e:
                                            st.error(f"Error archiving book: {str(e)}")

                                with col2:
                                    if st.button(
                                        f"Delete '{book_title}'",
                                        key=f"delete_progress_{book_title}",
                                        help="Permanently delete this book and all its data",
                                        type="secondary",
                                    ):
                                        # Add confirmation using session state
                                        confirm_key = f"confirm_delete_progress_{book_title}"
                                        if confirm_key not in st.session_state:
                                            st.session_state[confirm_key] = False

                                        if not st.session_state[confirm_key]:
                                            st.session_state[confirm_key] = True
                                            st.warning(
                                                f"Click 'Delete {book_title}' again to permanently delete all data for this book."
                                            )
                                        else:
                                            try:
                                                with engine.connect() as conn:
                                                    conn.execute(
                                                        text(
                                                            '''
                                                            DELETE FROM trello_time_tracking
                                                            WHERE card_name = :card_name
                                                        '''
                                                        ),
                                                        {'card_name': book_title},
                                                    )
                                                    conn.commit()

                                                # Reset confirmation state
                                                del st.session_state[confirm_key]
                                                # Keep user on the Book Progress tab
                                                st.success(f"'{book_title}' has been permanently deleted!")
                                                # Delete operation completed
                                            except Exception as e:
                                                st.error(f"Error deleting book: {str(e)}")
                                                # Reset confirmation state on error
                                                if confirm_key in st.session_state:
                                                    del st.session_state[confirm_key]

                            stage_counter += 1

                    # Pagination controls below book cards
                    total_pages = (
                        (total_books_to_display - 1) // books_per_page + 1 if total_books_to_display > 0 else 1
                    )
                    nav_col1, nav_col2 = st.columns(2)
                    with nav_col1:
                        if st.button("Previous", disabled=st.session_state.book_page == 0):
                            st.session_state.book_page -= 1
                            st.rerun()
                    with nav_col2:
                        if st.button("Next", disabled=st.session_state.book_page >= total_pages - 1):
                            st.session_state.book_page += 1
                            st.rerun()

        except SQLAlchemyError as e:
            timestamp = datetime.now(BST).strftime("%Y-%m-%d %H:%M:%S")

            st.session_state.error_log.append(
                {"time": timestamp, "message": f"Database error: {str(e)}"}
            )
            try:
                import traceback

                error_details = traceback.format_exc().split("\n")[-3:-1]
                st.session_state.error_log.append(
                    {"time": timestamp, "message": f"Location: {' '.join(error_details)}"}
                )
            except Exception:
                pass
            _original_st_error(
                "Database error, please see the error log for more details"
            )
        except Exception as e:
            timestamp = datetime.now(BST).strftime("%Y-%m-%d %H:%M:%S")
            error_message = f"{type(e).__name__}: {e}"
            st.session_state.error_log.append(

                {"time": timestamp, "message": str(e)}
            )
            try:
                import traceback

                error_details = traceback.format_exc().split("\n")[-3:-1]
                st.session_state.error_log.append(
                    {"time": timestamp, "message": f"Location: {' '.join(error_details)}"}
                )
            except Exception:
                pass
            _original_st_error(
                "An unexpected error occurred, please see the error log for more details"
            )

        # Add table showing all books with their boards below the book cards
        st.markdown("---")
        st.subheader("All Books Overview")

        # Create data for the table
        table_data = []

        # Create a dictionary to track books and their boards
        book_board_map = {}

        # First, add books with tasks from database
        if df_from_db is not None and not df_from_db.empty and 'Card name' in df_from_db.columns:
            try:
                for _, row in df_from_db.groupby('Card name').first().iterrows():
                    book_name = row['Card name']
                    board_name = row['Board'] if 'Board' in row and row['Board'] else 'Not set'
                    book_board_map[book_name] = board_name
            except Exception as e:
                # If groupby fails, fall back to simple iteration
                pass

        # Then add books without tasks from all_books
        try:
            for book_info in all_books:
                book_name = book_info[0]
                if book_name not in book_board_map:
                    board_name = book_info[1] if book_info[1] else 'Not set'
                    book_board_map[book_name] = board_name
        except Exception as e:
            # Handle case where all_books might be empty or malformed
            pass

        # Convert to sorted list for table display
        for book_name in sorted(book_board_map.keys()):
            table_data.append({'Book Name': book_name, 'Board': book_board_map[book_name]})

        if table_data:
            # Create DataFrame for display
            table_df = pd.DataFrame(table_data)
            st.dataframe(table_df, use_container_width=True, hide_index=True)
        else:
            st.info("No books found in the database.")

        # Clear refresh flags without automatic rerun to prevent infinite loops
        for flag in ['completion_changed', 'major_update_needed']:
            if flag in st.session_state:
                del st.session_state[flag]
                
        components.html(
        """
        <div style="text-align: center; margin-top: 10px;">
            <span style="font-size: 12px; color: #888; cursor: pointer; text-decoration: underline;"
                  onclick="document.getElementById('dont-click-modal').style.display='flex';">
                Please do not click
            </span>
        </div>
    
        <div id="dont-click-modal" style="display:none; position: fixed; top:0; left:0; width:100%; height:100%;
            background-color: rgba(0,0,0,0.5); z-index:1000; align-items: center; justify-content: center;">
          <div style="background-color: white; padding: 20px; border-radius: 8px; text-align: center; max-width: 300px;">
            <p style="margin-bottom: 20px;">What do you think you're doing? It clearly stated, 'Please do not click.'</p>
            <button onclick="document.getElementById('dont-click-modal').style.display='none';"
                    style="margin-right: 10px;">Go back</button>
            <button onclick="window.open('https://youtu.be/5T5BY1j2MkE', '_blank');">Proceed anyway</button>
          </div>
        </div>
        """,
        height=300,
    )
    with reporting_tab:
        st.header("Reporting")
        st.markdown("Filter tasks by user, book, board, tag, and date range from all uploaded data.")

        # Get filter options from database
        users = get_users_from_database(engine)
        books = get_books_from_database(engine)
        boards = get_boards_from_database(engine)
        tags = get_tags_from_database(engine)

        if not users:
            st.info("No users found in database. Please add entries in the 'Add Book' tab first.")
            st.stop()

        # Filter selection - organized in columns
        col1, col2 = st.columns(2)

        with col1:
            # User selection dropdown
            selected_user = st.selectbox(
                "Select User:", options=["All Users"] + users, help="Choose a user to view their tasks"
            )

            # Book search input
            book_search = st.text_input(
                "Search Book (optional):",
                placeholder="Start typing to search books...",
                help="Type to search for a specific book",
            )
            # Match the search to available books
            if book_search:
                matched_books = [book for book in books if book_search.lower() in book.lower()]
                if matched_books:
                    selected_book = st.selectbox(
                        "Select from matches:", options=matched_books, help="Choose from matching books"
                    )
                else:
                    st.warning("No books found matching your search")
                    selected_book = "All Books"
            else:
                selected_book = "All Books"

        with col2:
            # Board selection dropdown
            selected_board = st.selectbox(
                "Select Board (optional):", options=["All Boards"] + boards, help="Choose a specific board to filter by"
            )

            # Tag selection dropdown
            selected_tag = st.selectbox(
                "Select Tag (optional):", options=["All Tags"] + tags, help="Choose a specific tag to filter by"
            )

        # Date range selection
        col1, col2 = st.columns(2)
        with col1:
            start_date = st.date_input("Start Date (optional):", value=None, help="Leave empty to include all dates")

        with col2:
            end_date = st.date_input("End Date (optional):", value=None, help="Leave empty to include all dates")

        # Update button
        update_button = st.button("Update Table", type="primary")

        # Validate date range
        if start_date and end_date and start_date > end_date:
            st.error("Start date must be before end date")
            return

        # Filter and display results only when button is clicked or on initial load
        if update_button or 'filtered_tasks_displayed' not in st.session_state:
            with st.spinner("Loading filtered tasks..."):
                filtered_tasks = get_filtered_tasks_from_database(
                    engine,
                    user_name=selected_user if selected_user != "All Users" else None,
                    book_name=selected_book if selected_book != "All Books" else None,
                    board_name=selected_board if selected_board != "All Boards" else None,
                    tag_name=selected_tag if selected_tag != "All Tags" else None,
                    start_date=start_date,
                    end_date=end_date,
                )

            # Store in session state to prevent automatic reloading
            st.session_state.filtered_tasks_displayed = True
            st.session_state.current_filtered_tasks = filtered_tasks
            st.session_state.current_filters = {
                'user': selected_user,
                'book': selected_book,
                'board': selected_board,
                'tag': selected_tag,
                'start_date': start_date,
                'end_date': end_date,
            }

        # Display cached results if available
        if 'current_filtered_tasks' in st.session_state:

            filtered_tasks = st.session_state.current_filtered_tasks
            current_filters = st.session_state.get('current_filters', {})

            if not filtered_tasks.empty:
                st.subheader("Filtered Results")

                # Show active filters info
                active_filters = []
                if current_filters.get('user') and current_filters.get('user') != "All Users":
                    active_filters.append(f"User: {current_filters.get('user')}")
                if current_filters.get('book') and current_filters.get('book') != "All Books":
                    active_filters.append(f"Book: {current_filters.get('book')}")
                if current_filters.get('board') and current_filters.get('board') != "All Boards":
                    active_filters.append(f"Board: {current_filters.get('board')}")
                if current_filters.get('tag') and current_filters.get('tag') != "All Tags":
                    active_filters.append(f"Tag: {current_filters.get('tag')}")
                if current_filters.get('start_date') or current_filters.get('end_date'):
                    start_str = (
                        current_filters.get('start_date').strftime('%d/%m/%Y')
                        if current_filters.get('start_date')
                        else 'All'
                    )
                    end_str = (
                        current_filters.get('end_date').strftime('%d/%m/%Y')
                        if current_filters.get('end_date')
                        else 'All'
                    )
                    active_filters.append(f"Date range: {start_str} to {end_str}")

                if active_filters:
                    left_col, right_col = st.columns([1, 3])
                    with left_col:
                        with st.expander("Active Filters", expanded=False):
                            for f in active_filters:
                                st.write(f)
                    with right_col:
                        st.dataframe(filtered_tasks, use_container_width=True, hide_index=True)
                else:
                    st.dataframe(filtered_tasks, use_container_width=True, hide_index=True)

                # Download button for filtered results
                csv_buffer = io.StringIO()
                filtered_tasks.to_csv(csv_buffer, index=False)
                st.download_button(
                    label="Download Filtered Results",
                    data=csv_buffer.getvalue(),
                    file_name="filtered_tasks.csv",
                    mime="text/csv",
                )

                # Summary statistics for filtered data
                st.subheader("Summary")
                col1, col2, col3, col4 = st.columns(4)

                with col1:
                    st.metric("Total Books", int(filtered_tasks['Book Title'].nunique()))

                with col2:
                    st.metric("Total Tasks", len(filtered_tasks))

                with col3:
                    st.metric("Unique Users", int(filtered_tasks['User'].nunique()))

                with col4:
                    # Calculate total time from formatted time strings
                    total_seconds = 0
                    for time_str in filtered_tasks['Time Spent']:
                        if time_str != "00:00:00":
                            parts = time_str.split(':')
                            total_seconds += int(parts[0]) * 3600 + int(parts[1]) * 60 + int(parts[2])
                    total_hours = total_seconds / 3600
                    st.metric("Total Time (Hours)", f"{total_hours:.1f}")

            else:
                st.warning("No tasks found matching the selected filters.")

        elif 'filtered_tasks_displayed' not in st.session_state:
            st.info("Click 'Update Table' to load filtered results.")

    with archive_tab:
        st.header("Archive")
        st.markdown("View and manage archived books.")

        try:
            # Get count of archived records
            with engine.connect() as conn:
                archived_count = conn.execute(
                    text('SELECT COUNT(*) FROM trello_time_tracking WHERE archived = TRUE')
                ).scalar()

            if archived_count and archived_count > 0:
                st.info(f"Showing archived books from {archived_count} database records.")

                # Get archived data from database
                df_archived = pd.read_sql(
                    '''SELECT card_name as "Card name",
                       COALESCE(user_name, 'Not set') as "User",
                       list_name as "List",
                       time_spent_seconds as "Time spent (s)",
                       date_started as "Date started (f)",
                       card_estimate_seconds as "Card estimate(s)",
                       board_name as "Board", created_at, tag as "Tag"
                       FROM trello_time_tracking WHERE archived = TRUE ORDER BY created_at DESC''',
                    engine,
                )

                if not df_archived.empty:
                    # Add search bar for archived book titles
                    archive_search_query = st.text_input(
                        "Search archived books by title:",
                        placeholder="Enter book title to filter archived results...",
                        help="Search for specific archived books by typing part of the title",
                        key="archive_search",
                    )

                    # Filter archived books based on search
                    filtered_archived_df = df_archived.copy()
                    if archive_search_query:
                        mask = filtered_archived_df['Card name'].str.contains(
                            archive_search_query, case=False, na=False
                        )
                        filtered_archived_df = filtered_archived_df[mask]

                    # Get unique archived books
                    unique_archived_books = filtered_archived_df['Card name'].unique()

                    if len(unique_archived_books) > 0:
                        st.write(f"Found {len(unique_archived_books)} archived books to display")

                        # Display each archived book with same structure as Book Completion
                        for book_title in unique_archived_books:
                            book_mask = filtered_archived_df['Card name'] == book_title
                            book_data = filtered_archived_df[book_mask].copy()

                            # Calculate overall progress
                            total_time_spent = book_data['Time spent (s)'].sum()

                            # Calculate total estimated time
                            estimated_time = 0
                            if 'Card estimate(s)' in book_data.columns:
                                book_estimates = book_data['Card estimate(s)'].fillna(0).sum()
                                if book_estimates > 0:
                                    estimated_time = book_estimates

                            # Calculate completion percentage and progress text
                            if estimated_time > 0:
                                completion_percentage = (total_time_spent / estimated_time) * 100
                                progress_text = f"{format_seconds_to_time(total_time_spent)}/{format_seconds_to_time(estimated_time)} ({completion_percentage:.1f}%)"
                            else:
                                completion_percentage = 0
                                progress_text = f"Total: {format_seconds_to_time(total_time_spent)} (No estimate)"

                            with st.expander(book_title, expanded=False):
                                # Show progress bar and completion info at the top
                                progress_bar_html = f"""
                                <div style="width: 50%; background-color: #f0f0f0; border-radius: 5px; height: 10px; margin: 8px 0;">
                                    <div style="width: {min(completion_percentage, 100):.1f}%; background-color: #2AA395; height: 100%; border-radius: 5px;"></div>
                                </div>
                                """
                                st.markdown(progress_bar_html, unsafe_allow_html=True)
                                st.markdown(
                                    f'<div style="font-size: 14px; color: #666; margin-bottom: 10px;">{progress_text}</div>',
                                    unsafe_allow_html=True,
                                )

                                st.markdown("---")

                                # Show task breakdown for archived book
                                task_breakdown = (
                                    book_data.groupby(['List', 'User'])['Time spent (s)'].sum().reset_index()
                                )
                                task_breakdown['Time Spent'] = task_breakdown['Time spent (s)'].apply(
                                    format_seconds_to_time
                                )
                                task_breakdown = task_breakdown[['List', 'User', 'Time Spent']]

                                st.write("**Task Breakdown:**")
                                st.dataframe(task_breakdown, use_container_width=True, hide_index=True)

                                # Unarchive and Delete buttons
                                st.markdown("---")
                                col1, col2 = st.columns(2)

                                with col1:
                                    if st.button(
                                        f"Unarchive '{book_title}'",
                                        key=f"unarchive_{book_title}",
                                        help="Move this book back to active books",
                                    ):
                                        try:
                                            with engine.connect() as conn:
                                                conn.execute(
                                                    text(
                                                        '''
                                                    UPDATE trello_time_tracking
                                                    SET archived = FALSE
                                                    WHERE card_name = :card_name
                                                '''
                                                    ),
                                                    {'card_name': book_title},
                                                )
                                                conn.commit()

                                            # Keep user on the Archive tab
                                            st.success(f"'{book_title}' has been unarchived successfully!")
                                            st.rerun()
                                        except Exception as e:
                                            st.error(f"Error unarchiving book: {str(e)}")

                                with col2:
                                    if st.button(
                                        f"Delete '{book_title}'",
                                        key=f"delete_{book_title}",
                                        help="Permanently delete this book and all its data",
                                        type="secondary",
                                    ):
                                        # Add confirmation using session state
                                        confirm_key = f"confirm_delete_{book_title}"
                                        if confirm_key not in st.session_state:
                                            st.session_state[confirm_key] = False

                                        if not st.session_state[confirm_key]:
                                            st.session_state[confirm_key] = True
                                            st.warning(
                                                f"Click 'Delete {book_title}' again to permanently delete all data for this book."
                                            )
                                            st.rerun()
                                        else:
                                            try:
                                                with engine.connect() as conn:
                                                    conn.execute(
                                                        text(
                                                            '''
                                                        DELETE FROM trello_time_tracking
                                                        WHERE card_name = :card_name
                                                    '''
                                                        ),
                                                        {'card_name': book_title},
                                                    )
                                                    conn.commit()

                                                # Reset confirmation state
                                                del st.session_state[confirm_key]
                                                # Keep user on the Archive tab
                                                st.success(f"'{book_title}' has been permanently deleted!")
                                                st.rerun()
                                            except Exception as e:
                                                st.error(f"Error deleting book: {str(e)}")
                                                # Reset confirmation state on error
                                                if confirm_key in st.session_state:
                                                    del st.session_state[confirm_key]
                    else:
                        if archive_search_query:
                            st.warning(f"No archived books found matching '{archive_search_query}'")
                        else:
                            st.warning("No archived books available")
                else:
                    st.warning("No archived books available")
            else:
                st.info("No archived books found. Archive books from the 'Book Completion' tab to see them here.")

        except Exception as e:
            st.error(f"Error accessing archived data: {str(e)}")
    with error_log_tab:
        st.header("Error Log")
        password_input = st.text_input(
            "Enter password",
            type="password",
            key="error_log_password",
        )
        if password_input == "nan":
            if st.session_state.error_log:
                df_log = pd.DataFrame(st.session_state.error_log)
                st.dataframe(df_log, use_container_width=True, hide_index=True)
                csv = df_log.to_csv(index=False).encode("utf-8")
                st.download_button(
                    "Download Error Log",
                    csv,
                    "error_log.csv",
                    "text/csv",
                )
            else:
                st.info("No errors logged yet.")
        elif password_input:
            st.warning("Incorrect password")
        else:
            st.info("Enter password to view logs")

if __name__ == "__main__":
    main()
