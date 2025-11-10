import asyncio
import json
import pickle

async def receive_message(reader: asyncio.StreamReader) -> bytes:
    """
    Lit un message préfixé par 4 octets de taille (non-bloquant).
    C'est la fonction de base pour Pickle et JSON.
    """
    try:
        msg_length_bytes = await reader.readexactly(4)
        if not msg_length_bytes:
            return None
            
        size = int.from_bytes(msg_length_bytes, byteorder='big')
        
        message_data = await reader.readexactly(size)
        if not message_data:
            return None

        return message_data
        
    except (asyncio.IncompleteReadError, ConnectionResetError):
        print("[PROTO] Connexion perdue pendant la lecture.")
        return None

async def send_message(writer: asyncio.StreamWriter, message_data: bytes):
    """
    Envoie un message préfixé par 4 octets de taille (non-bloquant).
    C'est la fonction de base pour Pickle et JSON.
    """
    try:
        len_bytes = len(message_data).to_bytes(4, byteorder='big')
        writer.write(len_bytes)
        writer.write(message_data)
        await writer.drain()
        return True
        
    except (ConnectionError, BrokenPipeError):
        print("[PROTO] Connexion perdue pendant l'envoi.")
        return False

# --- Couche JSON (Pour le Contrôle Master <-> Worker) ---

async def send_json(writer: asyncio.StreamWriter, data_object: dict):
    """Sérialise un dict en JSON et l'envoie."""
    try:
        message_bytes = json.dumps(data_object).encode('utf-8')
        return await send_message(writer, message_bytes)
    except TypeError:
        print(f"[PROTO-JSON] Erreur: L'objet {data_object} n'est pas sérialisable en JSON.")
        return False

async def receive_json(reader: asyncio.StreamReader) -> dict | None:
    """Reçoit des données et les parse en JSON."""
    message_bytes = await receive_message(reader)
    if message_bytes:
        try:
            return json.loads(message_bytes.decode('utf-8'))
        except json.JSONDecodeError:
            print("[PROTO-JSON] Erreur: Message reçu n'est pas du JSON valide.")
            return None
    return None

# --- Couche Pickle (Pour les Données Worker <-> Worker) ---

async def send_pickle(writer: asyncio.StreamWriter, data_object: any):
    """Sérialise un objet Python en Pickle et l'envoie."""
    message_bytes = pickle.dumps(data_object)
    return await send_message(writer, message_bytes)

async def receive_pickle(reader: asyncio.StreamReader) -> any:
    """Reçoit des données et les dé-pickle."""
    message_bytes = await receive_message(reader)
    if message_bytes:
        try:
            return pickle.loads(message_bytes)
        except pickle.UnpicklingError:
            print("[PROTO-PICKLE] Erreur: Message reçu n'est pas du Pickle valide.")
            return None
    return None