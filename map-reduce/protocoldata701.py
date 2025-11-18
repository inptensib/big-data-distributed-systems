import asyncio
import json
import logging
import zlib  # <-- AJOUT POUR LA COMPRESSION
from typing import Union, Any

# import pickle # <-- VOLONTAIREMENT SUPPRIMÉ (Faille de sécurité)


async def receive_message(reader: asyncio.StreamReader) -> bytes:
    """
    Lit un message préfixé par 4 octets de taille (non-bloquant).
    """
    try:
        msg_length_bytes = await reader.readexactly(4)
        if not msg_length_bytes:
            return None
        size = int.from_bytes(msg_length_bytes, byteorder="big")
        message_data = await reader.readexactly(size)
        if not message_data:
            return None
        return message_data
    except (asyncio.IncompleteReadError, ConnectionResetError):
        return None
    except Exception as e:
        logging.error(f"[PROTO] Erreur de lecture inattendue: {e}")
        return None


async def send_message(writer: asyncio.StreamWriter, message_data: bytes):
    """
    Envoie un message préfixé par 4 octets de taille (non-bloquant).
    """
    try:
        len_bytes = len(message_data).to_bytes(4, "big")
        writer.write(len_bytes)
        writer.write(message_data)
        await writer.drain()
        return True
    except (ConnectionError, BrokenPipeError):
        return False
    except Exception as e:
        logging.error(f"[PROTO] Erreur d'envoi inattendue: {e}")
        return False


# --- Couche JSON (Pour le Contrôle Master <-> Worker) ---
# --- NON COMPRESSÉ ---
async def send_json(writer: asyncio.StreamWriter, data_object: Any):
    """Sérialise un objet (dict, list) en JSON et l'envoie."""
    try:
        message_bytes = json.dumps(data_object).encode("utf-8")
        return await send_message(writer, message_bytes)
    except TypeError:
        logging.error(
            f"[PROTO-JSON] Erreur: L'objet {data_object} n'est pas sérialisable en JSON."
        )
        return False


async def receive_json(reader: asyncio.StreamReader) -> Union[Any, None]:
    """Reçoit des données et les parse en JSON."""
    message_bytes = await receive_message(reader)
    if message_bytes:
        try:
            return json.loads(message_bytes.decode("utf-8"))
        except json.JSONDecodeError:
            logging.warning(
                "[PROTO-JSON] Erreur: Message reçu n'est pas du JSON valide."
            )
            return None
    return None


# -----------------------------------------------------------------
# ▼▼▼ NOUVELLE SECTION : JSON COMPRESSÉ (Pour le Shuffle Worker <-> Worker) ▼▼▼
# -----------------------------------------------------------------
async def send_compressed_json(writer: asyncio.StreamWriter, data_object: Any):
    """
    Sérialise un objet en JSON, le compresse avec zlib, et l'envoie.
    """
    try:
        # 1. Sérialiser en JSON
        json_bytes = json.dumps(data_object).encode("utf-8")

        # 2. Compresser
        compressed_bytes = zlib.compress(json_bytes)

        # Optionnel: logguer le gain de compression
        # logging.debug(f"[PROTO-ZLIB] Compression: {len(json_bytes)} -> {len(compressed_bytes)} octets")

        # 3. Envoyer les données compressées
        return await send_message(writer, compressed_bytes)

    except TypeError:
        logging.error(
            f"[PROTO-ZLIB] Erreur: L'objet {data_object} n'est pas sérialisable en JSON."
        )
        return False
    except Exception as e:
        logging.error(f"[PROTO-ZLIB] Erreur d'envoi compressé: {e}")
        return False


async def receive_compressed_json(reader: asyncio.StreamReader) -> Union[Any, None]:
    """
    Reçoit des données compressées, les décompresse, et les parse en JSON.
    """
    # 1. Recevoir le message (qui est compressé)
    compressed_bytes = await receive_message(reader)
    if not compressed_bytes:
        return None

    try:
        # 2. Décompresser
        json_bytes = zlib.decompress(compressed_bytes)

        # 3. Parser en JSON
        return json.loads(json_bytes.decode("utf-8"))

    except zlib.error:
        logging.warning("[PROTO-ZLIB] Erreur: Message reçu n'est pas du zlib valide.")
        return None
    except json.JSONDecodeError:
        logging.warning(
            "[PROTO-ZLIB] Erreur: Données décompressées ne sont pas du JSON valide."
        )
        return None
    except Exception as e:
        logging.error(f"[PROTO-ZLIB] Erreur de réception compressée: {e}")
        return None
