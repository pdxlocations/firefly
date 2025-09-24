#!/usr/bin/env python3

import uuid
from database import Database
from encryption import generate_hash

def restore_example_profiles():
    """Restore the example profiles that were lost"""
    
    db = Database()
    
    print("🔄 Restoring Example Profiles...")
    print("=" * 40)
    
    # Current profiles from the database (transferred from mudpchat2.db)
    example_profiles = [
        {
            "profile_id": "e7b312f4-1122-4a3a-baaa-12ff3ac0aa01",
            "node_id": "!2badc0de",
            "long_name": "Packet Whisperer",
            "short_name": "Wisp",
            "channel": "LongFast",
            "key": "AQ==",
            "hop_limit": 3
        },
        {
            "profile_id": "1a93fd12-2233-4a3a-bbbb-23fe4ad0bb02",
            "node_id": "!beefcafe",
            "long_name": "UDP Genie",
            "short_name": "Geni",
            "channel": "ShortFast",
            "key": "AQ==",
            "hop_limit": 2
        },
        {
            "profile_id": "2b84ec23-3344-4a3a-cccc-34ed5be0cc03",
            "node_id": "!cafebabe",
            "long_name": "Mesh Rider",
            "short_name": "Ridr",
            "channel": "ShortFast",
            "key": "AQ==",
            "hop_limit": 4
        },
        {
            "profile_id": "3c75db34-4455-4a3a-dddd-45dc6cf0dd04",
            "node_id": "!f00dbabe",
            "long_name": "Echo Phantom",
            "short_name": "Echo",
            "channel": "ShortFast",
            "key": "AQ==",
            "hop_limit": 3
        },
        {
            "profile_id": "4d66ca45-5566-4a3a-eeee-56cb7df0ee05",
            "node_id": "!decafbad",
            "long_name": "Ping Lord",
            "short_name": "Ping",
            "channel": "ShortFast",
            "key": "AQ==",
            "hop_limit": 5
        },
        {
            "profile_id": "5e57b956-6677-4a3a-ffff-67ba8ef0ff06",
            "node_id": "!0ddba11a",
            "long_name": "Captain Multicast",
            "short_name": "Capn",
            "channel": "ShortFast",
            "key": "AQ==",
            "hop_limit": 1
        },
        {
            "profile_id": "6f489867-7788-4a3a-aaaa-78a99ff0aa07",
            "node_id": "!facefeed",
            "long_name": "Byte Bandit",
            "short_name": "Band",
            "channel": "MediumFast",
            "key": "AQ==",
            "hop_limit": 3
        },
        {
            "profile_id": "7059a978-8899-4a3a-bbbb-89b0aff0bb08",
            "node_id": "!feedf00d",
            "long_name": "Signal Sprite",
            "short_name": "Sigl",
            "channel": "MeshOregon",
            "key": "AQ==",
            "hop_limit": 7
        }
    ]
    
    created_count = 0
    
    for profile_data in example_profiles:
        # Use the specific profile ID from the data
        profile_id = profile_data["profile_id"]
        
        success = db.create_profile(
            profile_id=profile_id,
            node_id=profile_data["node_id"],
            long_name=profile_data["long_name"],
            short_name=profile_data["short_name"],
            channel=profile_data["channel"],
            key=profile_data["key"],
            hop_limit=profile_data["hop_limit"]
        )
        
        if success:
            channel_num = generate_hash(profile_data["channel"], profile_data["key"])
            print(f"✅ {profile_data['long_name']} ({profile_data['channel']}) -> Channel {channel_num} | Hop Limit: {profile_data['hop_limit']}")
            created_count += 1
        else:
            print(f"❌ Failed to create {profile_data['long_name']}")
    
    print()
    print(f"🎉 Successfully restored {created_count} example profiles!")
    print()
    print("Channel Distribution:")
    print("  - ShortFast (Channel 112): 5 profiles (Capn, Ping, Echo, Ridr, Geni)")
    print("  - MeshOregon (Channel 15): 1 profile (Sigl)")
    print("  - MediumFast (Channel 31): 1 profile (Band)")
    print("  - LongFast (Channel 8): 1 profile (Wisp)")
    print()
    print("🚀 You can now start the application and select any profile!")
    print("   Command: python3 start_with_venv.py")

if __name__ == "__main__":
    restore_example_profiles()