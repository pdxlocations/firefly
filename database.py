#!/usr/bin/env python3

import sqlite3
import json
from datetime import datetime
from typing import Dict, List, Optional, Tuple
import os


class Database:
    def __init__(self, db_path: str = "mudpchat.db"):
        self.db_path = db_path
        self.init_database()

    def init_database(self):
        """Initialize the database with required tables"""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("PRAGMA foreign_keys = ON")
            
            # Profiles table (migrate from JSON file)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS profiles (
                    id TEXT PRIMARY KEY,
                    node_id TEXT NOT NULL,
                    long_name TEXT NOT NULL,
                    short_name TEXT NOT NULL,
                    channel TEXT NOT NULL,
                    key TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Nodes table - stores nodeinfo for nodes seen by each profile
            conn.execute("""
                CREATE TABLE IF NOT EXISTS nodes (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    profile_id TEXT NOT NULL,
                    node_id TEXT NOT NULL,
                    node_num INTEGER NOT NULL,
                    long_name TEXT,
                    short_name TEXT,
                    macaddr BLOB,
                    hw_model TEXT,
                    role TEXT,
                    public_key BLOB,
                    first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    packet_count INTEGER DEFAULT 1,
                    raw_nodeinfo TEXT,  -- JSON blob of the full nodeinfo payload
                    FOREIGN KEY (profile_id) REFERENCES profiles (id) ON DELETE CASCADE,
                    UNIQUE(profile_id, node_num)
                )
            """)
            
            # Messages table - stores all messages by channel (not by profile)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS messages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    message_id TEXT UNIQUE,  -- UUID for web UI
                    packet_id INTEGER,  -- Meshtastic packet ID
                    sender_num INTEGER,
                    sender_display TEXT,
                    content TEXT NOT NULL,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    sender_ip TEXT,
                    direction TEXT DEFAULT 'received',  -- 'sent' or 'received'
                    channel INTEGER NOT NULL,  -- Channel number (from generate_hash)
                    hop_limit INTEGER,
                    hop_start INTEGER,
                    rx_snr REAL,
                    rx_rssi INTEGER
                )
            """)
            
            # Profile read status table - tracks last seen timestamp for each profile+channel combination
            conn.execute("""
                CREATE TABLE IF NOT EXISTS profile_last_seen (
                    profile_id TEXT NOT NULL,
                    channel INTEGER NOT NULL,
                    last_seen_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (profile_id, channel),
                    FOREIGN KEY (profile_id) REFERENCES profiles (id) ON DELETE CASCADE
                )
            """)
            
            # Create indexes for better performance
            conn.execute("CREATE INDEX IF NOT EXISTS idx_nodes_profile_id ON nodes (profile_id)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_nodes_last_seen ON nodes (last_seen DESC)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_messages_channel ON messages (channel)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_messages_timestamp ON messages (timestamp DESC)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_messages_channel_timestamp ON messages (channel, timestamp DESC)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_profile_last_seen ON profile_last_seen (profile_id)")
            
            conn.commit()

    def migrate_profiles_from_json(self, json_file: str = "profiles.json"):
        """Migrate existing profiles from JSON file to database (only if not already migrated)"""
        if not os.path.exists(json_file):
            return False  # No JSON file to migrate
            
        try:
            # Check if we already have profiles in the database
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute("SELECT COUNT(*) FROM profiles")
                existing_count = cursor.fetchone()[0]
                
                if existing_count > 0:
                    print(f"Database already has {existing_count} profiles, skipping JSON migration")
                    return False
            
            # Load and migrate JSON profiles
            with open(json_file, 'r') as f:
                profiles_data = json.load(f)
                
            if not profiles_data:
                print("No profiles found in JSON file")
                return False
                
            with sqlite3.connect(self.db_path) as conn:
                migrated_count = 0
                for profile_id, profile in profiles_data.items():
                    conn.execute("""
                        INSERT OR IGNORE INTO profiles 
                        (id, node_id, long_name, short_name, channel, key, created_at, updated_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        profile_id,
                        profile.get('node_id', ''),
                        profile.get('long_name', ''),
                        profile.get('short_name', ''),
                        profile.get('channel', ''),
                        profile.get('key', ''),
                        profile.get('created_at', datetime.now().isoformat()),
                        profile.get('updated_at', datetime.now().isoformat())
                    ))
                    migrated_count += 1
                conn.commit()
                
                print(f"Successfully migrated {migrated_count} profiles from {json_file}")
                
                # Only backup the JSON file after successful migration
                backup_file = f"{json_file}.migrated"
                if not os.path.exists(backup_file):
                    os.rename(json_file, backup_file)
                    print(f"JSON profiles backed up as {backup_file}")
                    
                return True
                
        except Exception as e:
            print(f"Error migrating profiles: {e}")
            return False

    def get_all_profiles(self) -> Dict:
        """Get all profiles as dictionary (compatible with existing code)"""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute("""
                SELECT id, node_id, long_name, short_name, channel, key, 
                       created_at, updated_at FROM profiles ORDER BY created_at DESC
            """)
            profiles = {}
            for row in cursor:
                profiles[row['id']] = {
                    'id': row['id'],
                    'node_id': row['node_id'],
                    'long_name': row['long_name'],
                    'short_name': row['short_name'],
                    'channel': row['channel'],
                    'key': row['key'],
                    'created_at': row['created_at'],
                    'updated_at': row['updated_at']
                }
            return profiles

    def get_profile(self, profile_id: str) -> Optional[Dict]:
        """Get a specific profile"""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute("""
                SELECT id, node_id, long_name, short_name, channel, key, 
                       created_at, updated_at FROM profiles WHERE id = ?
            """, (profile_id,))
            row = cursor.fetchone()
            if row:
                return {
                    'id': row['id'],
                    'node_id': row['node_id'],
                    'long_name': row['long_name'],
                    'short_name': row['short_name'],
                    'channel': row['channel'],
                    'key': row['key'],
                    'created_at': row['created_at'],
                    'updated_at': row['updated_at']
                }
            return None

    def create_profile(self, profile_id: str, node_id: str, long_name: str, 
                      short_name: str, channel: str, key: str) -> bool:
        """Create a new profile"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("""
                    INSERT INTO profiles (id, node_id, long_name, short_name, channel, key)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (profile_id, node_id, long_name, short_name, channel, key))
                conn.commit()
                return True
        except Exception as e:
            print(f"Error creating profile: {e}")
            return False

    def update_profile(self, profile_id: str, node_id: str, long_name: str, 
                      short_name: str, channel: str, key: str) -> bool:
        """Update an existing profile"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute("""
                    UPDATE profiles SET node_id = ?, long_name = ?, short_name = ?, 
                                      channel = ?, key = ?, updated_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                """, (node_id, long_name, short_name, channel, key, profile_id))
                return cursor.rowcount > 0
        except Exception as e:
            print(f"Error updating profile: {e}")
            return False

    def delete_profile(self, profile_id: str) -> bool:
        """Delete a profile and all associated data"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute("DELETE FROM profiles WHERE id = ?", (profile_id,))
                return cursor.rowcount > 0
        except Exception as e:
            print(f"Error deleting profile: {e}")
            return False

    def store_node(self, profile_id: str, node_num: int, node_id: str, 
                   long_name: str = None, short_name: str = None, 
                   macaddr: bytes = None, hw_model: str = None, 
                   role: str = None, public_key: bytes = None, 
                   raw_nodeinfo: str = None) -> bool:
        """Store or update a node seen by a profile"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                # Check if node exists for this profile
                cursor = conn.execute("""
                    SELECT id, packet_count FROM nodes WHERE profile_id = ? AND node_num = ?
                """, (profile_id, node_num))
                existing = cursor.fetchone()
                
                if existing:
                    # Update existing node
                    conn.execute("""
                        UPDATE nodes SET 
                            node_id = COALESCE(?, node_id),
                            long_name = COALESCE(?, long_name),
                            short_name = COALESCE(?, short_name),
                            macaddr = COALESCE(?, macaddr),
                            hw_model = COALESCE(?, hw_model),
                            role = COALESCE(?, role),
                            public_key = COALESCE(?, public_key),
                            last_seen = CURRENT_TIMESTAMP,
                            packet_count = packet_count + 1,
                            raw_nodeinfo = COALESCE(?, raw_nodeinfo)
                        WHERE id = ?
                    """, (node_id, long_name, short_name, macaddr, hw_model, 
                         role, public_key, raw_nodeinfo, existing[0]))
                else:
                    # Insert new node
                    conn.execute("""
                        INSERT INTO nodes 
                        (profile_id, node_num, node_id, long_name, short_name, 
                         macaddr, hw_model, role, public_key, raw_nodeinfo)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (profile_id, node_num, node_id, long_name, short_name, 
                         macaddr, hw_model, role, public_key, raw_nodeinfo))
                conn.commit()
                return True
        except Exception as e:
            print(f"Error storing node: {e}")
            return False

    def get_nodes_for_profile(self, profile_id: str) -> List[Dict]:
        """Get all nodes seen by a specific profile"""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute("""
                SELECT node_num, node_id, long_name, short_name, macaddr, 
                       hw_model, role, first_seen, last_seen, packet_count, raw_nodeinfo
                FROM nodes WHERE profile_id = ? ORDER BY last_seen DESC
            """, (profile_id,))
            
            nodes = []
            for row in cursor:
                node_data = {
                    'node_num': row['node_num'],
                    'node_id': row['node_id'],
                    'long_name': row['long_name'],
                    'short_name': row['short_name'],
                    'hw_model': row['hw_model'],
                    'role': row['role'],
                    'first_seen': row['first_seen'],
                    'last_seen': row['last_seen'],
                    'packet_count': row['packet_count']
                }
                
                # Add raw nodeinfo if available
                if row['raw_nodeinfo']:
                    try:
                        node_data['raw_nodeinfo'] = json.loads(row['raw_nodeinfo'])
                    except:
                        pass
                        
                # Format MAC address if available
                if row['macaddr']:
                    try:
                        mac_bytes = row['macaddr']
                        node_data['macaddr'] = ':'.join(f'{b:02x}' for b in mac_bytes)
                    except:
                        pass
                        
                nodes.append(node_data)
            return nodes

    def store_message(self, message_id: str, packet_id: int = None,
                     sender_num: int = None, sender_display: str = None,
                     content: str = None, sender_ip: str = None, direction: str = "received",
                     channel: int = None, hop_limit: int = None, hop_start: int = None,
                     rx_snr: float = None, rx_rssi: int = None) -> bool:
        """Store a message by channel (accessible to any profile using that channel)"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("""
                    INSERT INTO messages 
                    (message_id, packet_id, sender_num, sender_display, 
                     content, sender_ip, direction, channel, hop_limit, hop_start, 
                     rx_snr, rx_rssi)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (message_id, packet_id, sender_num, sender_display,
                     content, sender_ip, direction, channel, hop_limit, hop_start,
                     rx_snr, rx_rssi))
                conn.commit()
                return True
        except Exception as e:
            print(f"Error storing message: {e}")
            return False

    # Legacy method - use get_messages_for_channel() instead
    def get_messages_for_profile(self, profile_id: str, limit: int = 100) -> List[Dict]:
        """Legacy method - messages are now stored by channel, not profile"""
        print(f"[DEPRECATED] get_messages_for_profile() called - use get_messages_for_channel() instead")
        return []

    def get_stats(self, profile_id: str = None) -> Dict:
        """Get database statistics"""
        with sqlite3.connect(self.db_path) as conn:
            if profile_id:
                # Get the profile's channel to count messages for that channel
                profile_cursor = conn.execute("""
                    SELECT channel, key FROM profiles WHERE id = ?
                """, (profile_id,))
                profile_row = profile_cursor.fetchone()
                
                if profile_row:
                    from encryption import generate_hash
                    channel = generate_hash(profile_row[0], profile_row[1])
                    
                    cursor = conn.execute("""
                        SELECT 
                            (SELECT COUNT(*) FROM nodes WHERE profile_id = ?) as node_count,
                            (SELECT COUNT(*) FROM messages WHERE channel = ?) as message_count
                    """, (profile_id, channel))
                else:
                    cursor = conn.execute("""
                        SELECT 
                            (SELECT COUNT(*) FROM nodes WHERE profile_id = ?) as node_count,
                            0 as message_count
                    """, (profile_id,))
            else:
                cursor = conn.execute("""
                    SELECT 
                        (SELECT COUNT(*) FROM profiles) as profile_count,
                        (SELECT COUNT(*) FROM nodes) as total_nodes,
                        (SELECT COUNT(*) FROM messages) as total_messages
                """)
            
            row = cursor.fetchone()
            if profile_id:
                return {
                    'nodes': row[0],
                    'messages': row[1]
                }
            else:
                return {
                    'profiles': row[0],
                    'total_nodes': row[1], 
                    'total_messages': row[2]
                }

    def get_messages_for_channel(self, channel: int, limit: int = 100) -> List[Dict]:
        """Get messages for a specific channel (accessible to any profile using that channel)"""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            
            # Get messages for the specific channel
            cursor = conn.execute("""
                SELECT message_id, packet_id, sender_num, sender_display, content, 
                       timestamp, sender_ip, direction, channel, hop_limit, hop_start,
                       rx_snr, rx_rssi
                FROM messages WHERE channel = ?
                ORDER BY timestamp DESC LIMIT ?
            """, (channel, limit))
            
            messages = []
            for row in cursor:
                sender_num = row['sender_num']
                
                # Use stored sender_display or fallback
                if row['sender_display']:
                    current_sender_display = row['sender_display']
                elif sender_num:
                    current_sender_display = f"!{hex(sender_num)[2:].zfill(8)}"
                else:
                    current_sender_display = 'Unknown'
                
                messages.append({
                    'id': row['message_id'],
                    'packet_id': row['packet_id'],
                    'sender': f"!{hex(sender_num)[2:].zfill(8)}" if sender_num else 'Unknown',
                    'sender_display': current_sender_display,
                    'content': row['content'],
                    'timestamp': row['timestamp'],
                    'sender_ip': row['sender_ip'],
                    'direction': row['direction'],
                    'channel': row['channel'],
                    'hop_limit': row['hop_limit'],
                    'hop_start': row['hop_start'],
                    'rx_snr': row['rx_snr'],
                    'rx_rssi': row['rx_rssi']
                })
            return messages[::-1]  # Return in chronological order

    def get_nodes_for_profile_channel(self, profile_id: str, channel: int) -> List[Dict]:
        """Get nodes for a specific profile that have been seen on a specific channel"""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            
            # Get nodes that have sent messages on the specified channel
            cursor = conn.execute("""
                SELECT DISTINCT n.node_id, n.node_num, n.long_name, n.short_name, 
                       n.macaddr, n.hw_model, n.role, n.first_seen, n.last_seen, 
                       n.packet_count, n.raw_nodeinfo
                FROM nodes n
                WHERE n.profile_id = ? 
                AND EXISTS (
                    SELECT 1 FROM messages m 
                    WHERE m.sender_num = n.node_num AND m.channel = ?
                )
                ORDER BY n.last_seen DESC
            """, (profile_id, channel))
            
            nodes = []
            for row in cursor:
                node_data = {
                    'node_id': row['node_id'],
                    'node_num': row['node_num'],
                    'long_name': row['long_name'],
                    'short_name': row['short_name'],
                    'hw_model': row['hw_model'],
                    'role': row['role'],
                    'first_seen': row['first_seen'],
                    'last_seen': row['last_seen'],
                    'packet_count': row['packet_count']
                }
                
                # Try to parse raw_nodeinfo JSON
                if row['raw_nodeinfo']:
                    try:
                        node_data['raw_nodeinfo'] = json.loads(row['raw_nodeinfo'])
                    except:
                        pass
                        
                # Format MAC address if available
                if row['macaddr']:
                    try:
                        mac_bytes = row['macaddr']
                        node_data['macaddr'] = ':'.join(f'{b:02x}' for b in mac_bytes)
                    except:
                        pass
                        
                nodes.append(node_data)
            return nodes
    
    def update_profile_last_seen(self, profile_id: str, channel: int) -> bool:
        """Update the last seen timestamp for a profile+channel combination"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                # Update or insert last seen record
                conn.execute("""
                    INSERT OR REPLACE INTO profile_last_seen 
                    (profile_id, channel, last_seen_timestamp)
                    VALUES (?, ?, CURRENT_TIMESTAMP)
                """, (profile_id, channel))
                conn.commit()
                return True
        except Exception as e:
            print(f"Error updating profile last seen: {e}")
            return False
    
    def get_unread_messages_for_channel(self, profile_id: str, channel: int, limit: int = 100) -> List[Dict]:
        """Get unread messages for a specific channel (based on profile's last seen timestamp)"""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            
            # Get the last seen timestamp for this profile+channel combination
            cursor = conn.execute("""
                SELECT last_seen_timestamp FROM profile_last_seen 
                WHERE profile_id = ? AND channel = ?
            """, (profile_id, channel))
            last_seen_row = cursor.fetchone()
            
            if last_seen_row and last_seen_row['last_seen_timestamp']:
                # Get messages newer than last seen
                cursor = conn.execute("""
                    SELECT message_id, packet_id, sender_num, sender_display, content, 
                           timestamp, sender_ip, direction, channel, hop_limit, hop_start,
                           rx_snr, rx_rssi
                    FROM messages WHERE channel = ? AND timestamp > ?
                    ORDER BY timestamp DESC LIMIT ?
                """, (channel, last_seen_row['last_seen_timestamp'], limit))
            else:
                # No last seen timestamp - return all messages (but limit to prevent overload)
                cursor = conn.execute("""
                    SELECT message_id, packet_id, sender_num, sender_display, content, 
                           timestamp, sender_ip, direction, channel, hop_limit, hop_start,
                           rx_snr, rx_rssi
                    FROM messages WHERE channel = ?
                    ORDER BY timestamp DESC LIMIT ?
                """, (channel, limit))
            
            messages = []
            for row in cursor:
                sender_num = row['sender_num']
                
                # Use stored sender_display or fallback
                if row['sender_display']:
                    current_sender_display = row['sender_display']
                elif sender_num:
                    current_sender_display = f"!{hex(sender_num)[2:].zfill(8)}"
                else:
                    current_sender_display = 'Unknown'
                
                messages.append({
                    'id': row['message_id'],
                    'packet_id': row['packet_id'],
                    'sender': f"!{hex(sender_num)[2:].zfill(8)}" if sender_num else 'Unknown',
                    'sender_display': current_sender_display,
                    'content': row['content'],
                    'timestamp': row['timestamp'],
                    'sender_ip': row['sender_ip'],
                    'direction': row['direction'],
                    'channel': row['channel'],
                    'hop_limit': row['hop_limit'],
                    'hop_start': row['hop_start'],
                    'rx_snr': row['rx_snr'],
                    'rx_rssi': row['rx_rssi']
                })
            return messages[::-1]  # Return in chronological order
