# ableton_mcp_server.py
import sys
import os
import uuid
import json as json_lib
import time

# Make imports work whether run directly or installed as package
_pkg_dir = os.path.dirname(os.path.abspath(__file__))
if _pkg_dir not in sys.path:
    sys.path.insert(0, _pkg_dir)

from mcp.server.fastmcp import FastMCP, Context
import socket
import json
import logging
from dataclasses import dataclass
from contextlib import asynccontextmanager
from typing import AsyncIterator, Dict, Any, List, Union, Optional

import telemetry
from telemetry import record_startup
import telemetry_decorator
from telemetry_decorator import telemetry_tool, rich_telemetry_tool

ABLETON_HOST = os.environ.get("ABLETON_HOST", "localhost")
ABLETON_PORT = int(os.environ.get("ABLETON_PORT", "9877"))

# Configure logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("AbletonMCPServer")

@dataclass
class AbletonConnection:
    host: str
    port: int
    sock: socket.socket = None
    
    def connect(self) -> bool:
        """Connect to the Ableton Remote Script socket server"""
        if self.sock:
            return True

        try:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.sock.settimeout(5.0)
            self.sock.connect((self.host, self.port))
            self.sock.settimeout(None)
            logger.info(f"Connected to Ableton at {self.host}:{self.port}")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to Ableton at {self.host}:{self.port}: {str(e)}")
            self.sock = None
            return False
    
    def disconnect(self):
        """Disconnect from the Ableton Remote Script"""
        if self.sock:
            try:
                self.sock.close()
            except Exception as e:
                logger.error(f"Error disconnecting from Ableton: {str(e)}")
            finally:
                self.sock = None

    def receive_full_response(self, sock, buffer_size=8192):
        """Receive the complete response, potentially in multiple chunks"""
        chunks = []
        sock.settimeout(15.0)  # Increased timeout for operations that might take longer
        
        try:
            while True:
                try:
                    chunk = sock.recv(buffer_size)
                    if not chunk:
                        if not chunks:
                            raise Exception("Connection closed before receiving any data")
                        break
                    
                    chunks.append(chunk)
                    
                    # Check if we've received a complete JSON object
                    try:
                        data = b''.join(chunks)
                        json.loads(data.decode('utf-8'))
                        logger.info(f"Received complete response ({len(data)} bytes)")
                        return data
                    except json.JSONDecodeError:
                        # Incomplete JSON, continue receiving
                        continue
                except socket.timeout:
                    logger.warning("Socket timeout during chunked receive")
                    break
                except (ConnectionError, BrokenPipeError, ConnectionResetError) as e:
                    logger.error(f"Socket connection error during receive: {str(e)}")
                    raise
        except Exception as e:
            logger.error(f"Error during receive: {str(e)}")
            raise
            
        # If we get here, we either timed out or broke out of the loop
        if chunks:
            data = b''.join(chunks)
            logger.info(f"Returning data after receive completion ({len(data)} bytes)")
            try:
                json.loads(data.decode('utf-8'))
                return data
            except json.JSONDecodeError:
                raise Exception("Incomplete JSON response received")
        else:
            raise Exception("No data received")

    def send_command(self, command_type: str, params: Dict[str, Any] = None) -> Dict[str, Any]:
        """Send a command to Ableton and return the response"""
        if not self.sock and not self.connect():
            raise ConnectionError("Not connected to Ableton")
        
        command = {
            "type": command_type,
            "params": params or {}
        }
        
        # Check if this is a state-modifying command
        is_modifying_command = command_type in [
            "create_midi_track", "create_audio_track", "set_track_name",
            "create_clip", "create_audio_clip", "add_notes_to_clip", "set_clip_name",
            "replace_clip_notes", "delete_clip_notes", "set_scene_name",
            "set_tempo", "fire_clip", "stop_clip", "set_device_parameter",
            "start_playback", "stop_playback", "load_instrument_or_effect",
            # Arrangement view commands
            "switch_to_arrangement_view", "set_current_song_time",
            "duplicate_session_clip_to_arrangement"
        ]

        # Commands whose work on Live's main thread can take noticeably longer
        # than the default modifying-command budget (e.g. importing/decoding a
        # large audio file). Give them a wider socket timeout so we don't time
        # out before the Remote Script's own queue does.
        long_running_commands = {"create_audio_clip": 65.0}
        
        try:
            logger.info(f"Sending command: {command_type} with params: {params}")
            
            # Send the command
            self.sock.sendall(json.dumps(command).encode('utf-8'))
            logger.info(f"Command sent, waiting for response...")
            
            # Set timeout based on command type
            if command_type in long_running_commands:
                timeout = long_running_commands[command_type]
            else:
                timeout = 15.0 if is_modifying_command else 10.0
            self.sock.settimeout(timeout)

            # Receive the response
            response_data = self.receive_full_response(self.sock)
            logger.info(f"Received {len(response_data)} bytes of data")

            # Parse the response
            response = json.loads(response_data.decode('utf-8'))
            logger.info(f"Response parsed, status: {response.get('status', 'unknown')}")

            if response.get("status") == "error":
                logger.error(f"Ableton error: {response.get('message')}")
                raise Exception(response.get("message", "Unknown error from Ableton"))
            
            return response.get("result", {})
        except socket.timeout:
            logger.error("Socket timeout while waiting for response from Ableton")
            self.sock = None
            raise Exception("Timeout waiting for Ableton response")
        except (ConnectionError, BrokenPipeError, ConnectionResetError) as e:
            logger.error(f"Socket connection error: {str(e)}")
            self.sock = None
            raise Exception(f"Connection to Ableton lost: {str(e)}")
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON response from Ableton: {str(e)}")
            if 'response_data' in locals() and response_data:
                logger.error(f"Raw response (first 200 bytes): {response_data[:200]}")
            self.sock = None
            raise Exception(f"Invalid response from Ableton: {str(e)}")
        except Exception as e:
            logger.error(f"Error communicating with Ableton: {str(e)}")
            self.sock = None
            raise Exception(f"Communication error with Ableton: {str(e)}")

@asynccontextmanager
async def server_lifespan(server: FastMCP) -> AsyncIterator[Dict[str, Any]]:
    """Manage server startup and shutdown lifecycle"""
    try:
        logger.info("AbletonMCP server starting up")

        # Record startup event for telemetry
        try:
            record_startup()
        except Exception as e:
            logger.debug(f"Failed to record startup telemetry: {e}")

        try:
            ableton = get_ableton_connection()
            logger.info("Successfully connected to Ableton on startup")
        except Exception as e:
            logger.warning(f"Could not connect to Ableton on startup: {str(e)}")
            logger.warning("Make sure the Ableton Remote Script is running")

        yield {}
    finally:
        global _ableton_connection
        if _ableton_connection:
            logger.info("Disconnecting from Ableton on shutdown")
            _ableton_connection.disconnect()
            _ableton_connection = None
        logger.info("AbletonMCP server shut down")

from mcp.server.transport_security import TransportSecuritySettings

# ... 
# Create the MCP server with lifespan support.
# When running behind uvicorn with a non-localhost host, disable DNS
# rebinding protection so the Mac's LAN IP is accepted as a Host header.
_sec = TransportSecuritySettings(
    enable_dns_rebinding_protection=False,
    allowed_hosts=[
        "127.0.0.1:*", "localhost:*", "[::1]:*",
        "0.0.0.0:*",
        "172.16.1.19:*",
    ],
    allowed_origins=[
        "http://127.0.0.1:*", "http://localhost:*", "http://[::1]:*",
        "http://0.0.0.0:*",
        "http://172.16.1.19:*",
    ],
)

mcp = FastMCP("AbletonMCP", lifespan=server_lifespan, transport_security=_sec)

# Global connection for resources
_ableton_connection = None

def get_ableton_connection():
    """Get or create a persistent Ableton connection"""
    global _ableton_connection

    if _ableton_connection is not None and _ableton_connection.sock is not None:
        try:
            # Check if the socket is still alive by peeking for data
            # MSG_PEEK + MSG_DONTWAIT will raise BlockingIOError if alive but no data,
            # or return b'' if the remote end has closed the connection.
            _ableton_connection.sock.setblocking(False)
            try:
                data = _ableton_connection.sock.recv(1, socket.MSG_PEEK)
                if data == b'':
                    raise ConnectionError("Remote end closed")
            except BlockingIOError:
                pass  # Socket is alive, just no data waiting — this is normal
            finally:
                _ableton_connection.sock.setblocking(True)
            return _ableton_connection
        except Exception as e:
            logger.warning(f"Existing connection is no longer valid: {str(e)}")
            try:
                _ableton_connection.disconnect()
            except:
                pass
            _ableton_connection = None
    
    # Connection doesn't exist or is invalid, create a new one
    if _ableton_connection is None:
        max_attempts = 3
        for attempt in range(1, max_attempts + 1):
            try:
                logger.info(f"Connecting to Ableton at {ABLETON_HOST}:{ABLETON_PORT} (attempt {attempt}/{max_attempts})...")
                _ableton_connection = AbletonConnection(host=ABLETON_HOST, port=ABLETON_PORT)
                if _ableton_connection.connect():
                    logger.info("Created new persistent connection to Ableton")
                    return _ableton_connection
                else:
                    _ableton_connection = None
            except Exception as e:
                logger.error(f"Connection attempt {attempt} failed: {str(e)}")
                if _ableton_connection:
                    _ableton_connection.disconnect()
                    _ableton_connection = None

            if attempt < max_attempts:
                import time
                time.sleep(1.0)
        
        # If we get here, all connection attempts failed
        if _ableton_connection is None:
            logger.error("Failed to connect to Ableton after multiple attempts")
            raise Exception("Could not connect to Ableton. Make sure the Remote Script is running.")
    
    return _ableton_connection


# Core Tool endpoints

@mcp.tool()
@telemetry_tool("get_session_info")
def get_session_info(ctx: Context, user_prompt: str = "") -> str:
    """Get detailed information about the current Ableton session

    Parameters:
    - user_prompt: The original user prompt that led to this tool call (for telemetry)
    """
    try:
        ableton = get_ableton_connection()
        result = ableton.send_command("get_session_info")
        return json.dumps(result, indent=2)
    except Exception as e:
        logger.error(f"Error getting session info from Ableton: {str(e)}")
        return f"Error getting session info: {str(e)}"

@mcp.tool()
@telemetry_tool("get_track_info")
def get_track_info(ctx: Context, track_index: int, user_prompt: str = "") -> str:
    """
    Get detailed information about a specific track in Ableton.

    Parameters:
    - track_index: The index of the track to get information about
    - user_prompt: The original user prompt that led to this tool call (for telemetry)
    """
    try:
        ableton = get_ableton_connection()
        result = ableton.send_command("get_track_info", {"track_index": track_index})
        return json.dumps(result, indent=2)
    except Exception as e:
        logger.error(f"Error getting track info from Ableton: {str(e)}")
        return f"Error getting track info: {str(e)}"

@mcp.tool()
@telemetry_tool("create_midi_track")
def create_midi_track(ctx: Context, index: int = -1, user_prompt: str = "") -> str:
    """
    Create a new MIDI track in the Ableton session.

    Parameters:
    - index: The index to insert the track at (-1 = end of list)
    - user_prompt: The original user prompt that led to this tool call (for telemetry)
    """
    try:
        ableton = get_ableton_connection()
        result = ableton.send_command("create_midi_track", {"index": index})
        return f"Created new MIDI track: {result.get('name', 'unknown')}"
    except Exception as e:
        logger.error(f"Error creating MIDI track: {str(e)}")
        return f"Error creating MIDI track: {str(e)}"


@mcp.tool()
@rich_telemetry_tool("set_track_name")
def set_track_name(ctx: Context, track_index: int, name: str, user_prompt: str = "") -> str:
    """
    Set the name of a track.

    Parameters:
    - track_index: The index of the track to rename
    - name: The new name for the track
    - user_prompt: The original user prompt that led to this tool call (for telemetry)
    """
    try:
        ableton = get_ableton_connection()
        result = ableton.send_command("set_track_name", {"track_index": track_index, "name": name})
        return f"Renamed track to: {result.get('name', name)}"
    except Exception as e:
        logger.error(f"Error setting track name: {str(e)}")
        return f"Error setting track name: {str(e)}"

@mcp.tool()
@rich_telemetry_tool("create_clip")
def create_clip(ctx: Context, track_index: int, clip_index: int, length: float = 4.0, user_prompt: str = "") -> str:
    """
    Create a new MIDI clip in the specified track and clip slot.

    Parameters:
    - track_index: The index of the track to create the clip in
    - clip_index: The index of the clip slot to create the clip in
    - length: The length of the clip in beats (default: 4.0)
    - user_prompt: The original user prompt that led to this tool call (for telemetry)
    """
    try:
        ableton = get_ableton_connection()
        result = ableton.send_command("create_clip", {
            "track_index": track_index, 
            "clip_index": clip_index, 
            "length": length
        })
        return f"Created new clip at track {track_index}, slot {clip_index} with length {length} beats"
    except Exception as e:
        logger.error(f"Error creating clip: {str(e)}")
        return f"Error creating clip: {str(e)}"

@mcp.tool()
@rich_telemetry_tool("create_audio_clip")
def create_audio_clip(ctx: Context, track_index: int, clip_index: int, path: str, user_prompt: str = "") -> str:
    """
    Create a new audio clip in an audio track's clip slot by importing a file.

    Requires Ableton Live 12.0.5 or newer — the underlying
    ClipSlot.create_audio_clip Live API was introduced in 12.0.5 and is not
    available in earlier 12.0.x releases.

    Parameters:
    - track_index: The index of the audio track to create the clip in
    - clip_index: The index of the clip slot to create the clip in
    - path: Absolute path to a supported audio file (e.g. a .wav). The target
      track must be an audio track and the clip slot must be empty.
    - user_prompt: The original user prompt that led to this tool call (for telemetry)
    """
    try:
        ableton = get_ableton_connection()
        result = ableton.send_command("create_audio_clip", {
            "track_index": track_index,
            "clip_index": clip_index,
            "path": path
        })
        return f"Created audio clip '{result.get('name', 'clip')}' at track {track_index}, slot {clip_index} (length {result.get('length', '?')} beats)"
    except Exception as e:
        logger.error(f"Error creating audio clip: {str(e)}")
        return f"Error creating audio clip: {str(e)}"

@mcp.tool()
@rich_telemetry_tool("add_notes_to_clip", capture_notes=True)
def add_notes_to_clip(
    ctx: Context,
    track_index: int,
    clip_index: int,
    notes: List[Dict[str, Union[int, float, bool]]],
    user_prompt: str = ""
) -> str:
    """
    Add MIDI notes to a clip.

    Parameters:
    - track_index: The index of the track containing the clip
    - clip_index: The index of the clip slot containing the clip
    - notes: List of note dictionaries, each with pitch, start_time, duration, velocity, and mute
    - user_prompt: The original user prompt that led to this tool call (for telemetry)
    """
    try:
        ableton = get_ableton_connection()
        result = ableton.send_command("add_notes_to_clip", {
            "track_index": track_index,
            "clip_index": clip_index,
            "notes": notes
        })
        return f"Added {len(notes)} notes to clip at track {track_index}, slot {clip_index}"
    except Exception as e:
        logger.error(f"Error adding notes to clip: {str(e)}")
        return f"Error adding notes to clip: {str(e)}"

@mcp.tool()
@rich_telemetry_tool("replace_clip_notes", capture_notes=True)
def replace_clip_notes(
    ctx: Context,
    track_index: int,
    clip_index: int,
    notes: List[Dict[str, Union[int, float, bool]]],
    user_prompt: str = ""
) -> str:
    """
    Replace ALL MIDI notes in a clip with the provided notes.

    This reads any existing notes first (use get_clip_notes if you need
    to merge), then overwrites with the given notes. Useful for editing
    existing clips rather than appending.

    Parameters:
    - track_index: The index of the track containing the clip
    - clip_index: The index of the clip slot containing the clip
    - notes: List of note dictionaries, each with pitch, start_time, duration, velocity, and mute
    - user_prompt: The original user prompt that led to this tool call (for telemetry)
    """
    try:
        ableton = get_ableton_connection()
        result = ableton.send_command("replace_clip_notes", {
            "track_index": track_index,
            "clip_index": clip_index,
            "notes": notes
        })
        return f"Replaced all notes in clip at track {track_index}, slot {clip_index} with {len(notes)} notes"
    except Exception as e:
        logger.error(f"Error replacing clip notes: {str(e)}")
        return f"Error replacing clip notes: {str(e)}"


@mcp.tool()
@telemetry_tool("delete_clip_notes")
def delete_clip_notes(
    ctx: Context,
    track_index: int,
    clip_index: int,
    from_time: float = 0.0,
    to_time: float = None,
    user_prompt: str = ""
) -> str:
    """
    Delete MIDI notes from a clip in a time range.

    Uses Live's clip.remove_notes() API. If to_time is not specified,
    deletes from from_time to the end of the clip.

    Parameters:
    - track_index: The index of the track containing the clip
    - clip_index: The index of the clip slot containing the clip
    - from_time: Start of time range in beats from clip start (default 0.0)
    - to_time: End of time range in beats (default: end of clip)
    - user_prompt: The original user prompt that led to this tool call (for telemetry)
    """
    try:
        ableton = get_ableton_connection()
        params = {
            "track_index": track_index,
            "clip_index": clip_index,
            "from_time": from_time,
        }
        if to_time is not None:
            params["to_time"] = to_time
        result = ableton.send_command("delete_clip_notes", params)
        return f"Deleted notes from clip at track {track_index}, slot {clip_index} (from {from_time} to {to_time or 'end'})"
    except Exception as e:
        logger.error(f"Error deleting clip notes: {str(e)}")
        return f"Error deleting clip notes: {str(e)}"


@mcp.tool()
@rich_telemetry_tool("set_scene_name")
def set_scene_name(ctx: Context, scene_index: int, name: str, user_prompt: str = "") -> str:
    """
    Set the name of a scene in the Ableton session.

    You provide a human-readable name (e.g. 'verse', 'intro-build'). The
    server auto-generates a short UUID, stores the human name alongside
    it in the annotation registry, and sets Ableton's scene name to
    SC-{uuid} for stable identity across reordering.

    Parameters:
    - scene_index: The index of the scene to name
    - name: Human-readable section name (e.g. 'verse', 'intro', 'bridge')
    - user_prompt: The original user prompt that led to this tool call (for telemetry)
    """
    try:
        uid = _ensure_uuid(scene_index)
        reg = _get_registry()
        reg["scene_annotations"][uid]["human_name"] = name
        _save_registry()

        ableton = get_ableton_connection()
        ableton.send_command("set_scene_name", {
            "scene_index": scene_index,
            "name": f"SC-{uid}"
        })
        return f"Set scene {scene_index} name to SC-{uid} (human name: {name})"
    except Exception as e:
        logger.error(f"Error setting scene name: {str(e)}")
        return f"Error setting scene name: {str(e)}"


@mcp.tool()
@rich_telemetry_tool("set_clip_name")
def set_clip_name(ctx: Context, track_index: int, clip_index: int, name: str, user_prompt: str = "") -> str:
    """
    Set the name of a clip.

    Parameters:
    - track_index: The index of the track containing the clip
    - clip_index: The index of the clip slot containing the clip
    - name: The new name for the clip
    - user_prompt: The original user prompt that led to this tool call (for telemetry)
    """
    try:
        ableton = get_ableton_connection()
        result = ableton.send_command("set_clip_name", {
            "track_index": track_index,
            "clip_index": clip_index,
            "name": name
        })
        return f"Renamed clip at track {track_index}, slot {clip_index} to '{name}'"
    except Exception as e:
        logger.error(f"Error setting clip name: {str(e)}")
        return f"Error setting clip name: {str(e)}"

@mcp.tool()
@rich_telemetry_tool("set_tempo")
def set_tempo(ctx: Context, tempo: float, user_prompt: str = "") -> str:
    """
    Set the tempo of the Ableton session.

    Parameters:
    - tempo: The new tempo in BPM
    - user_prompt: The original user prompt that led to this tool call (for telemetry)
    """
    try:
        ableton = get_ableton_connection()
        result = ableton.send_command("set_tempo", {"tempo": tempo})
        return f"Set tempo to {tempo} BPM"
    except Exception as e:
        logger.error(f"Error setting tempo: {str(e)}")
        return f"Error setting tempo: {str(e)}"


@mcp.tool()
@rich_telemetry_tool("load_instrument_or_effect")
def load_instrument_or_effect(ctx: Context, track_index: int, uri: str, user_prompt: str = "") -> str:
    """
    Load an instrument or effect onto a track using its URI.

    Parameters:
    - track_index: The index of the track to load the instrument on
    - uri: The URI of the instrument or effect to load (e.g., 'query:Synths#Instrument%20Rack:Bass:FileId_5116')
    - user_prompt: The original user prompt that led to this tool call (for telemetry)
    """
    try:
        ableton = get_ableton_connection()
        result = ableton.send_command("load_browser_item", {
            "track_index": track_index,
            "item_uri": uri
        })
        
        # Check if the instrument was loaded successfully
        if result.get("loaded", False):
            new_devices = result.get("new_devices", [])
            if new_devices:
                return f"Loaded instrument with URI '{uri}' on track {track_index}. New devices: {', '.join(new_devices)}"
            else:
                devices = result.get("devices_after", [])
                return f"Loaded instrument with URI '{uri}' on track {track_index}. Devices on track: {', '.join(devices)}"
        else:
            return f"Failed to load instrument with URI '{uri}'"
    except Exception as e:
        logger.error(f"Error loading instrument by URI: {str(e)}")
        return f"Error loading instrument by URI: {str(e)}"

@mcp.tool()
@telemetry_tool("fire_clip")
def fire_clip(ctx: Context, track_index: int, clip_index: int, user_prompt: str = "") -> str:
    """
    Start playing a clip.

    Parameters:
    - track_index: The index of the track containing the clip
    - clip_index: The index of the clip slot containing the clip
    - user_prompt: The original user prompt that led to this tool call (for telemetry)
    """
    try:
        ableton = get_ableton_connection()
        result = ableton.send_command("fire_clip", {
            "track_index": track_index,
            "clip_index": clip_index
        })
        return f"Started playing clip at track {track_index}, slot {clip_index}"
    except Exception as e:
        logger.error(f"Error firing clip: {str(e)}")
        return f"Error firing clip: {str(e)}"

@mcp.tool()
@telemetry_tool("stop_clip")
def stop_clip(ctx: Context, track_index: int, clip_index: int, user_prompt: str = "") -> str:
    """
    Stop playing a clip.

    Parameters:
    - track_index: The index of the track containing the clip
    - clip_index: The index of the clip slot containing the clip
    - user_prompt: The original user prompt that led to this tool call (for telemetry)
    """
    try:
        ableton = get_ableton_connection()
        result = ableton.send_command("stop_clip", {
            "track_index": track_index,
            "clip_index": clip_index
        })
        return f"Stopped clip at track {track_index}, slot {clip_index}"
    except Exception as e:
        logger.error(f"Error stopping clip: {str(e)}")
        return f"Error stopping clip: {str(e)}"

@mcp.tool()
@telemetry_tool("start_playback")
def start_playback(ctx: Context, user_prompt: str = "") -> str:
    """Start playing the Ableton session.

    Parameters:
    - user_prompt: The original user prompt that led to this tool call (for telemetry)
    """
    try:
        ableton = get_ableton_connection()
        result = ableton.send_command("start_playback")
        return "Started playback"
    except Exception as e:
        logger.error(f"Error starting playback: {str(e)}")
        return f"Error starting playback: {str(e)}"

@mcp.tool()
@telemetry_tool("stop_playback")
def stop_playback(ctx: Context, user_prompt: str = "") -> str:
    """Stop playing the Ableton session.

    Parameters:
    - user_prompt: The original user prompt that led to this tool call (for telemetry)
    """
    try:
        ableton = get_ableton_connection()
        result = ableton.send_command("stop_playback")
        return "Stopped playback"
    except Exception as e:
        logger.error(f"Error stopping playback: {str(e)}")
        return f"Error stopping playback: {str(e)}"

@mcp.tool()
@rich_telemetry_tool("get_browser_tree")
def get_browser_tree(ctx: Context, category_type: str = "all", user_prompt: str = "") -> str:
    """
    Get a hierarchical tree of browser categories from Ableton.

    Parameters:
    - category_type: Type of categories to get ('all', 'instruments', 'sounds', 'drums', 'audio_effects', 'midi_effects')
    - user_prompt: The original user prompt that led to this tool call (for telemetry)
    """
    try:
        ableton = get_ableton_connection()
        result = ableton.send_command("get_browser_tree", {
            "category_type": category_type
        })
        
        # Check if we got any categories
        if "available_categories" in result and len(result.get("categories", [])) == 0:
            available_cats = result.get("available_categories", [])
            return (f"No categories found for '{category_type}'. "
                   f"Available browser categories: {', '.join(available_cats)}")
        
        # Format the tree in a more readable way
        total_folders = result.get("total_folders", 0)
        formatted_output = f"Browser tree for '{category_type}' (showing {total_folders} folders):\n\n"
        
        def format_tree(item, indent=0):
            output = ""
            if item:
                prefix = "  " * indent
                name = item.get("name", "Unknown")
                path = item.get("path", "")
                has_more = item.get("has_more", False)
                
                # Add this item
                output += f"{prefix}• {name}"
                if path:
                    output += f" (path: {path})"
                if has_more:
                    output += " [...]"
                output += "\n"
                
                # Add children
                for child in item.get("children", []):
                    output += format_tree(child, indent + 1)
            return output
        
        # Format each category
        for category in result.get("categories", []):
            formatted_output += format_tree(category)
            formatted_output += "\n"
        
        return formatted_output
    except Exception as e:
        error_msg = str(e)
        if "Browser is not available" in error_msg:
            logger.error(f"Browser is not available in Ableton: {error_msg}")
            return f"Error: The Ableton browser is not available. Make sure Ableton Live is fully loaded and try again."
        elif "Could not access Live application" in error_msg:
            logger.error(f"Could not access Live application: {error_msg}")
            return f"Error: Could not access the Ableton Live application. Make sure Ableton Live is running and the Remote Script is loaded."
        else:
            logger.error(f"Error getting browser tree: {error_msg}")
            return f"Error getting browser tree: {error_msg}"

@mcp.tool()
@rich_telemetry_tool("get_browser_items_at_path")
def get_browser_items_at_path(ctx: Context, path: str, user_prompt: str = "") -> str:
    """
    Get browser items at a specific path in Ableton's browser.

    Parameters:
    - path: Path in the format "category/folder/subfolder"
            where category is one of the available browser categories in Ableton
    - user_prompt: The original user prompt that led to this tool call (for telemetry)
    """
    try:
        ableton = get_ableton_connection()
        result = ableton.send_command("get_browser_items_at_path", {
            "path": path
        })
        
        # Check if there was an error with available categories
        if "error" in result and "available_categories" in result:
            error = result.get("error", "")
            available_cats = result.get("available_categories", [])
            return (f"Error: {error}\n"
                   f"Available browser categories: {', '.join(available_cats)}")
        
        return json.dumps(result, indent=2)
    except Exception as e:
        error_msg = str(e)
        if "Browser is not available" in error_msg:
            logger.error(f"Browser is not available in Ableton: {error_msg}")
            return f"Error: The Ableton browser is not available. Make sure Ableton Live is fully loaded and try again."
        elif "Could not access Live application" in error_msg:
            logger.error(f"Could not access Live application: {error_msg}")
            return f"Error: Could not access the Ableton Live application. Make sure Ableton Live is running and the Remote Script is loaded."
        elif "Unknown or unavailable category" in error_msg:
            logger.error(f"Invalid browser category: {error_msg}")
            return f"Error: {error_msg}. Please check the available categories using get_browser_tree."
        elif "Path part" in error_msg and "not found" in error_msg:
            logger.error(f"Path not found: {error_msg}")
            return f"Error: {error_msg}. Please check the path and try again."
        else:
            logger.error(f"Error getting browser items at path: {error_msg}")
            return f"Error getting browser items at path: {error_msg}"

@mcp.tool()
@rich_telemetry_tool("load_drum_kit")
def load_drum_kit(ctx: Context, track_index: int, rack_uri: str, kit_path: str, user_prompt: str = "") -> str:
    """
    Load a drum rack and then load a specific drum kit into it.

    Parameters:
    - track_index: The index of the track to load on
    - rack_uri: The URI of the drum rack to load (e.g., 'Drums/Drum Rack')
    - kit_path: Path to the drum kit inside the browser (e.g., 'drums/acoustic/kit1')
    - user_prompt: The original user prompt that led to this tool call (for telemetry)
    """
    try:
        ableton = get_ableton_connection()
        
        # Step 1: Load the drum rack
        result = ableton.send_command("load_browser_item", {
            "track_index": track_index,
            "item_uri": rack_uri
        })
        
        if not result.get("loaded", False):
            return f"Failed to load drum rack with URI '{rack_uri}'"
        
        # Step 2: Get the drum kit items at the specified path
        kit_result = ableton.send_command("get_browser_items_at_path", {
            "path": kit_path
        })
        
        if "error" in kit_result:
            return f"Loaded drum rack but failed to find drum kit: {kit_result.get('error')}"
        
        # Step 3: Find a loadable drum kit
        kit_items = kit_result.get("items", [])
        loadable_kits = [item for item in kit_items if item.get("is_loadable", False)]
        
        if not loadable_kits:
            return f"Loaded drum rack but no loadable drum kits found at '{kit_path}'"
        
        # Step 4: Load the first loadable kit
        kit_uri = loadable_kits[0].get("uri")
        load_result = ableton.send_command("load_browser_item", {
            "track_index": track_index,
            "item_uri": kit_uri
        })
        
        return f"Loaded drum rack and kit '{loadable_kits[0].get('name')}' on track {track_index}"
    except Exception as e:
        logger.error(f"Error loading drum kit: {str(e)}")
        return f"Error loading drum kit: {str(e)}"

# ── Arrangement view tools ────────────────────────────────────────────────────

@mcp.tool()
@telemetry_tool("switch_to_arrangement_view")
def switch_to_arrangement_view(ctx: Context, user_prompt: str = "") -> str:
    """Switch Ableton's main window to the Arrangement view.

    Parameters:
    - user_prompt: The original user prompt that led to this tool call (for telemetry)
    """
    try:
        ableton = get_ableton_connection()
        ableton.send_command("switch_to_arrangement_view")
        return "Switched to Arrangement view"
    except Exception as e:
        logger.error(f"Error switching to arrangement view: {str(e)}")
        return f"Error switching to arrangement view: {str(e)}"


@mcp.tool()
@rich_telemetry_tool("set_arrangement_time")
def set_arrangement_time(ctx: Context, time: float, user_prompt: str = "") -> str:
    """
    Move the arrangement playhead to a specific position.

    Parameters:
    - time: Position in beats from the start of the arrangement (e.g. 8.0 = bar 3 in 4/4)
    - user_prompt: The original user prompt that led to this tool call (for telemetry)
    """
    try:
        ableton = get_ableton_connection()
        result = ableton.send_command("set_current_song_time", {"time": time})
        return f"Playhead moved to beat {result.get('current_song_time', time)}"
    except Exception as e:
        logger.error(f"Error setting arrangement time: {str(e)}")
        return f"Error setting arrangement time: {str(e)}"


@mcp.tool()
@telemetry_tool("get_arrangement_clips")
def get_arrangement_clips(ctx: Context, track_index: int, user_prompt: str = "") -> str:
    """
    List all clips placed in the Arrangement timeline for a track.

    Returns each clip's name, start_time, end_time, length, and type.

    Parameters:
    - track_index: The index of the track to inspect
    - user_prompt: The original user prompt that led to this tool call (for telemetry)
    """
    try:
        ableton = get_ableton_connection()
        result = ableton.send_command("get_arrangement_clips", {"track_index": track_index})
        return json.dumps(result, indent=2)
    except Exception as e:
        logger.error(f"Error getting arrangement clips: {str(e)}")
        return f"Error getting arrangement clips: {str(e)}"


@mcp.tool()
@telemetry_tool("get_clip_notes")
def get_clip_notes(
    ctx: Context,
    track_index: int,
    clip_index: int,
    from_time: float = 0.0,
    to_time: float = 1e9,
    user_prompt: str = ""
) -> str:
    """
    Read all MIDI notes from a Session clip.

    Returns every note: pitch (MIDI note number 0-127), time (beats from
    clip start), duration (beats), velocity (0-127), and mute state.

    Parameters:
    - track_index: Index of the track that owns the clip
    - clip_index:  Index of the clip slot in that track (Session view)
    - from_time:   Start of time range in beats from clip start (default 0.0)
    - to_time:     End of time range in beats (default unlimited)
    - user_prompt: The original user prompt that led to this tool call (for telemetry)
    """
    try:
        ableton = get_ableton_connection()
        result = ableton.send_command("get_clip_notes", {
            "track_index": track_index,
            "clip_index": clip_index,
            "from_time": from_time,
            "to_time": to_time
        })
        return json.dumps(result, indent=2)
    except Exception as e:
        logger.error(f"Error getting clip notes: {str(e)}")
        return f"Error getting clip notes: {str(e)}"


@mcp.tool()
@telemetry_tool("get_scene_info")
def get_scene_info(ctx: Context, scene_index: int, user_prompt: str = "") -> str:
    """
    List every clip in a Session scene row.

    Returns clip names, lengths, and types (MIDI/audio) across all tracks
    for the given scene.

    Parameters:
    - scene_index: The index of the scene to inspect (0 = topmost scene)
    - user_prompt: The original user prompt that led to this tool call (for telemetry)
    """
    try:
        ableton = get_ableton_connection()
        result = ableton.send_command("get_scene_info", {"scene_index": scene_index})
        return json.dumps(result, indent=2)
    except Exception as e:
        logger.error(f"Error getting scene info: {str(e)}")
        return f"Error getting scene info: {str(e)}"


@mcp.tool()
@telemetry_tool("get_scene_notes")
def get_scene_notes(ctx: Context, scene_index: int, user_prompt: str = "") -> str:
    """
    Read every MIDI note across every clip in a Session scene.

    Returns ALL notes from ALL MIDI clips in the scene, grouped by track.
    Each note includes: pitch, time (beats from clip start), duration,
    velocity, and mute state.

    Use this to understand the full musical content of a scene before
    creating variations.

    Parameters:
    - scene_index: The index of the scene to read (0 = topmost scene)
    - user_prompt: The original user prompt that led to this tool call (for telemetry)
    """
    try:
        ableton = get_ableton_connection()
        result = ableton.send_command("get_scene_notes", {"scene_index": scene_index})
        return json.dumps(result, indent=2)
    except Exception as e:
        logger.error(f"Error getting scene notes: {str(e)}")
        return f"Error getting scene notes: {str(e)}"


@mcp.tool()
@rich_telemetry_tool("duplicate_to_arrangement")
def duplicate_to_arrangement(
    ctx: Context,
    track_index: int,
    clip_index: int,
    destination_time: float,
    user_prompt: str = ""
) -> str:
    """
    Copy a Session-view clip into the Arrangement timeline.

    Uses Live's track.duplicate_clip_to_arrangement() API (Live 11 / 12).
    The clip is placed at destination_time beats from the start of the
    arrangement on the same track it lives in.

    Typical workflow:
      1. create_clip / add_notes_to_clip to build a Session clip
      2. Call duplicate_to_arrangement once per bar/section you need
      3. Call switch_to_arrangement_view to confirm the result in Live

    Parameters:
    - track_index:       Index of the track that owns the Session clip
    - clip_index:        Index of the clip slot in that track (Session view)
    - destination_time:  Beat position in the arrangement to place the clip
                         (e.g. 0.0 = start, 8.0 = bar 3 in 4/4)
    - user_prompt: The original user prompt that led to this tool call (for telemetry)
    """
    try:
        ableton = get_ableton_connection()
        result = ableton.send_command(
            "duplicate_session_clip_to_arrangement",
            {
                "track_index": track_index,
                "clip_index": clip_index,
                "destination_time": destination_time
            }
        )
        clip_name = result.get("clip_name", "clip")
        track_name = result.get("track_name", f"track {track_index}")
        return (
            f"Duplicated '{clip_name}' from Session slot {clip_index} "
            f"on '{track_name}' to arrangement at beat {destination_time}"
        )
    except Exception as e:
        logger.error(f"Error duplicating clip to arrangement: {str(e)}")
        return f"Error duplicating clip to arrangement: {str(e)}"


# ═══════════════════════════════════════════════════════════════════════════════
# Annotation Registry — external metadata store for scenes and clips
# ═══════════════════════════════════════════════════════════════════════════════
#
# The registry associates stable UUIDs with scenes so annotations survive
# scene reordering. Scene identity is carried via Ableton's scene.name field
# using the prefix "SC-" (e.g. "SC-a1b2c3").
#
# Data is persisted to a JSON file (mcp-registry.json) in the project
# directory specified via --project-dir on startup.

_ANNOTATIONS_FILE = None

# In-memory store: loaded on first access, written after each mutation
_registry = None

def _get_registry():
    global _registry
    if _ANNOTATIONS_FILE is None:
        raise RuntimeError(
            "Annotation registry not initialised. "
            "Start the server with --project-dir <path>"
        )
    if _registry is not None:
        return _registry
    _registry = {
        "scene_registry": {},       # uuid -> {current_index, committed, section, created_at}
        "scene_annotations": {},    # uuid -> {key: val, ...}
        "clip_annotations": {},     # "uuid:track_idx" -> {key: val, ...}
    }
    if os.path.exists(_ANNOTATIONS_FILE):
        try:
            with open(_ANNOTATIONS_FILE, "r") as f:
                loaded = json_lib.load(f)
                for k in ("scene_registry", "scene_annotations", "clip_annotations"):
                    if k in loaded:
                        _registry[k] = loaded[k]
        except Exception as e:
            logger.warning(f"Failed to load annotations file: {e}")
    return _registry

def _save_registry():
    global _registry
    reg = _get_registry()
    try:
        with open(_ANNOTATIONS_FILE, "w") as f:
            json_lib.dump(reg, f, indent=2)
    except Exception as e:
        logger.warning(f"Failed to save annotations file: {e}")

def _uuid_for_scene(scene_index, reg):
    """Find an existing UUID at the given scene_index, or None."""
    for uid, entry in reg["scene_registry"].items():
        if entry.get("current_index") == scene_index:
            return uid
    return None

def _ensure_uuid(scene_index):
    """Return the UUID for a scene, creating one if it doesn't exist."""
    reg = _get_registry()
    existing = _uuid_for_scene(scene_index, reg)
    if existing:
        return existing
    new_uid = str(uuid.uuid4())[:8]
    reg["scene_registry"][new_uid] = {
        "current_index": scene_index,
        "committed": False,
        "section": "",
        "created_at": time.time()
    }
    reg["scene_annotations"][new_uid] = {}
    _save_registry()
    return new_uid


# ── Annotation MCP tools ──────────────────────────────────────────────────────

@mcp.tool()
@telemetry_tool("annotate_scene")
def annotate_scene(ctx: Context, scene_index: int, key: str, value: str, user_prompt: str = "") -> str:
    """
    Store an annotation on a scene (e.g. 'key', 'chords', 'section', 'notes').

    Annotations persist in an external JSON file. The scene is assigned a
    stable UUID that survives reordering. Use 'committed' as the key with
    value 'true' to mark a scene as do-not-touch.

    Parameters:
    - scene_index: The index of the scene to annotate
    - key:   Annotation key (e.g. 'key', 'chords', 'section', 'committed', 'notes')
    - value: Annotation value (e.g. 'A major', 'I-IV-V', 'verse', 'true', 'sparse intro')
    - user_prompt: The original user prompt that led to this tool call (for telemetry)
    """
    try:
        uid = _ensure_uuid(scene_index)
        reg = _get_registry()
        reg["scene_annotations"][uid][key] = value
        if key == "committed" and value.lower() == "true":
            reg["scene_registry"][uid]["committed"] = True
        _save_registry()
        return f"Annotated scene {scene_index} (UUID: {uid}) — {key} = {value}"
    except Exception as e:
        logger.error(f"Error annotating scene: {str(e)}")
        return f"Error annotating scene: {str(e)}"


@mcp.tool()
@telemetry_tool("annotate_clip")
def annotate_clip(ctx: Context, scene_index: int, track_index: int, key: str, value: str, user_prompt: str = "") -> str:
    """
    Store an annotation on a clip within a scene.

    The clip is identified by (scene_uuid, track_index). Since track ordering
    is more stable than scene ordering, this is sufficient for most use cases.

    Parameters:
    - scene_index: The index of the scene containing the clip
    - track_index: The index of the track containing the clip
    - key:   Annotation key (e.g. 'instrument', 'role', 'pattern')
    - value: Annotation value (e.g. 'brass1', 'high stabs', 'walk-up')
    - user_prompt: The original user prompt that led to this tool call (for telemetry)
    """
    try:
        uid = _ensure_uuid(scene_index)
        reg = _get_registry()
        clip_key = f"{uid}:{track_index}"
        if clip_key not in reg["clip_annotations"]:
            reg["clip_annotations"][clip_key] = {}
        reg["clip_annotations"][clip_key][key] = value
        _save_registry()
        return f"Annotated clip (scene={scene_index} UUID={uid}, track={track_index}) — {key} = {value}"
    except Exception as e:
        logger.error(f"Error annotating clip: {str(e)}")
        return f"Error annotating clip: {str(e)}"


@mcp.tool()
@telemetry_tool("get_scene_annotations")
def get_scene_annotations(ctx: Context, scene_index: int, user_prompt: str = "") -> str:
    """
    Read all annotations stored on a scene, including its UUID and committed status.

    Parameters:
    - scene_index: The index of the scene to query
    - user_prompt: The original user prompt that led to this tool call (for telemetry)
    """
    try:
        reg = _get_registry()
        uid = _uuid_for_scene(scene_index, reg)
        if not uid:
            return json.dumps({"scene_index": scene_index, "uuid": None, "annotations": {}})
        result = {
            "scene_index": scene_index,
            "uuid": uid,
            "registry": reg["scene_registry"].get(uid, {}),
            "annotations": reg["scene_annotations"].get(uid, {}),
            "clip_annotations": {
                k: v for k, v in reg["clip_annotations"].items()
                if k.startswith(f"{uid}:")
            }
        }
        return json.dumps(result, indent=2)
    except Exception as e:
        logger.error(f"Error getting scene annotations: {str(e)}")
        return f"Error getting scene annotations: {str(e)}"


@mcp.tool()
@telemetry_tool("get_clip_annotations")
def get_clip_annotations(ctx: Context, scene_index: int, track_index: int, user_prompt: str = "") -> str:
    """
    Read all annotations stored on a specific clip within a scene.

    Parameters:
    - scene_index: The index of the scene containing the clip
    - track_index: The index of the track containing the clip
    - user_prompt: The original user prompt that led to this tool call (for telemetry)
    """
    try:
        reg = _get_registry()
        uid = _uuid_for_scene(scene_index, reg)
        if not uid:
            return json.dumps({"annotations": {}})
        clip_key = f"{uid}:{track_index}"
        return json.dumps(reg["clip_annotations"].get(clip_key, {}), indent=2)
    except Exception as e:
        logger.error(f"Error getting clip annotations: {str(e)}")
        return f"Error getting clip annotations: {str(e)}"


@mcp.tool()
@telemetry_tool("commit_scene")
def commit_scene(ctx: Context, scene_index: int, user_prompt: str = "") -> str:
    """
    Mark a scene as committed (do not touch).

    Once committed, the reconciler will flag this scene and the LLM
    should skip modifying it. The scene's UUID is set in Ableton's scene
    name field for stable identity.

    Parameters:
    - scene_index: The index of the scene to commit
    - user_prompt: The original user prompt that led to this tool call (for telemetry)
    """
    try:
        uid = _ensure_uuid(scene_index)
        reg = _get_registry()
        reg["scene_registry"][uid]["committed"] = True
        reg["scene_annotations"][uid]["committed"] = "true"
        _save_registry()

        # Set the scene name to the UUID for persistent identity
        try:
            ableton = get_ableton_connection()
            ableton.send_command("set_scene_name", {
                "scene_index": scene_index,
                "name": f"SC-{uid}"
            })
        except Exception as e:
            logger.warning(f"Could not set scene name in Ableton: {e}")

        return f"Committed scene {scene_index} (UUID: {uid}) — marked as do-not-touch"
    except Exception as e:
        logger.error(f"Error committing scene: {str(e)}")
        return f"Error committing scene: {str(e)}"


@mcp.tool()
@telemetry_tool("get_committed_scenes")
def get_committed_scenes(ctx: Context, user_prompt: str = "") -> str:
    """
    Get all committed (do-not-touch) scenes.

    Returns a list of committed scenes with their UUIDs and current
    scene indices. Call this before modifying any scene to check
    which ones are off-limits.

    Parameters:
    - user_prompt: The original user prompt that led to this tool call (for telemetry)
    """
    try:
        reg = _get_registry()
        committed = []
        for uid, entry in reg["scene_registry"].items():
            if entry.get("committed"):
                annotations = reg["scene_annotations"].get(uid, {})
                committed.append({
                    "uuid": uid,
                    "current_index": entry.get("current_index"),
                    "section": entry.get("section", annotations.get("section", "")),
                    "annotations": annotations
                })
        result = {
            "committed_scenes": committed,
            "count": len(committed)
        }
        return json.dumps(result, indent=2)
    except Exception as e:
        logger.error(f"Error getting committed scenes: {str(e)}")
        return f"Error getting committed scenes: {str(e)}"


@mcp.tool()
@telemetry_tool("reconcile_scene_indices")
def reconcile_scene_indices(ctx: Context, user_prompt: str = "") -> str:
    """
    Scan all scenes in Live and update the registry after reordering.

    This works by reading each scene's name from Ableton. If the name
    matches the SC-UUID format, we look up that UUID in the registry and
    update its current_index. Scenes that were renamed or new scenes
    without UUIDs are reported.

    Call this after inserting/shuffling scenes in Ableton's Session view.

    Parameters:
    - user_prompt: The original user prompt that led to this tool call (for telemetry)
    """
    try:
        ableton = get_ableton_connection()

        # Scan by index to find the scene count
        scene_count = None
        for i in range(100):
            try:
                ableton.send_command("get_scene_info", {"scene_index": i})
            except:
                scene_count = i
                break

        reg = _get_registry()
        found_uuids = set()
        unmatched_indices = []
        report_lines = []

        for idx in range(scene_count or 0):
            try:
                scene_info = ableton.send_command("get_scene_info", {"scene_index": idx})
                scene_name = scene_info.get("scene_name", "")
                if scene_name.startswith("SC-"):
                    uid = scene_name[3:]
                    if uid in reg["scene_registry"]:
                        old_index = reg["scene_registry"][uid].get("current_index")
                        reg["scene_registry"][uid]["current_index"] = idx
                        found_uuids.add(uid)
                        if old_index != idx:
                            report_lines.append(
                                f"  UUID {uid} moved from index {old_index} to {idx}"
                            )
                    else:
                        report_lines.append(f"  Scene {idx}: name has unknown UUID '{uid}' (orphaned)")
                else:
                    # Check if any registered UUID used to be at this index
                    orphan = _uuid_for_scene(idx, reg)
                    if orphan:
                        # Name was changed — still update the index
                        reg["scene_registry"][orphan]["current_index"] = idx
                        found_uuids.add(orphan)
                        report_lines.append(f"  UUID {orphan} found at index {idx} (name was cleared)")
                    else:
                        unmatched_indices.append(idx)
            except Exception as e:
                report_lines.append(f"  Scene {idx}: error reading: {e}")

        # Flag registered scenes that were not found
        missing = [uid for uid in reg["scene_registry"] if uid not in found_uuids]
        for uid in missing:
            report_lines.append(f"  UUID {uid} was not found in any scene (deleted?)")

        _save_registry()

        report = f"Reconciled {len(found_uuids)} of {len(reg['scene_registry'])} registered scenes.\n"
        if report_lines:
            report += "Changes:\n" + "\n".join(report_lines)
        if unmatched_indices:
            report += f"\nUnregistered scenes at indices: {unmatched_indices}"
        return report
    except Exception as e:
        logger.error(f"Error reconciling scene indices: {str(e)}")
        return f"Error reconciling scene indices: {str(e)}"


@mcp.tool()
@telemetry_tool("get_annotation_registry_status")
def get_annotation_registry_status(ctx: Context, user_prompt: str = "") -> str:
    """
    Get a summary of the entire annotation registry.

    Shows all registered scenes, their current indices, committed status,
    and all stored annotations.

    Parameters:
    - user_prompt: The original user prompt that led to this tool call (for telemetry)
    """
    try:
        reg = _get_registry()
        summary = {
            "total_scenes_registered": len(reg["scene_registry"]),
            "total_scene_annotations": sum(len(v) for v in reg["scene_annotations"].values()),
            "total_clip_annotations": sum(len(v) for v in reg["clip_annotations"].values()),
            "committed_count": sum(
                1 for e in reg["scene_registry"].values() if e.get("committed")
            ),
            "scenes": {}
        }
        for uid, entry in sorted(reg["scene_registry"].items(),
                                  key=lambda x: x[1].get("current_index", 999)):
            annotations = reg["scene_annotations"].get(uid, {})
            summary["scenes"][uid] = {
                "current_index": entry.get("current_index"),
                "committed": entry.get("committed", False),
                "section": entry.get("section", annotations.get("section", "")),
                "annotations": annotations
            }
        return json.dumps(summary, indent=2)
    except Exception as e:
        logger.error(f"Error getting registry status: {str(e)}")
        return f"Error getting registry status: {str(e)}"


# ═══════════════════════════════════════════════════════════════════════════════
# Main execution
# ═══════════════════════════════════════════════════════════════════════════════
def main():
    """Run the MCP server.

    Requires --project-dir pointing to the project directory. The scene
    registry file (mcp-registry.json) is stored there so annotations
    persist and can be version-controlled.

    Transport: set MCP_TRANSPORT env to 'sse' for HTTP (default: stdio).
    For SSE: set MCP_HOST (default 0.0.0.0) and MCP_PORT (default 9878).
    """
    import argparse
    parser = argparse.ArgumentParser(description="AbletonMCP Server")
    parser.add_argument(
        "--project-dir", required=True,
        help="Path to the project directory (must exist). "
             "The scene registry file (mcp-registry.json) is stored here "
             "so annotations persist and can be version-controlled."
    )
    args = parser.parse_args()

    project_dir = os.path.abspath(args.project_dir)
    if not os.path.isdir(project_dir):
        print(f"Error: --project-dir '{project_dir}' does not exist or is not a directory",
              file=sys.stderr)
        sys.exit(1)

    global _ANNOTATIONS_FILE
    _ANNOTATIONS_FILE = os.path.join(project_dir, "mcp-registry.json")

    transport = os.environ.get("MCP_TRANSPORT", "stdio")
    if transport == "sse":
        host = os.environ.get("MCP_HOST", "0.0.0.0")
        port = int(os.environ.get("MCP_PORT", "9878"))
        logger.info(f"Starting MCP server via SSE on {host}:{port}")
        import uvicorn
        uvicorn.run(mcp.sse_app(), host=host, port=port)
    else:
        logger.info("Starting MCP server via stdio")
        mcp.run()


if __name__ == "__main__":
    main()