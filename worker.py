import asyncio
import argparse
import protocoldata701 as protocol  # Utilisation de votre nom de module
import os  # Pour la suppression de fichier (os.remove)
import logging  # Importer le module de logging
import sys  # Importer sys pour le stdout
import time  # Importer time pour le fallback job_id
import re
import zlib


# --- Déclarations Globales ---
peer_workers = {}
MY_SHUFFLE_PORT = 9001
MY_WORKER_ID = -1
MY_HOST_ADDRESS = "unknown_host"  # 1. AJOUTÉ ICI
SHUFFLE_MODE = "disk"

# Variables pour le mode RAM
intermediate_data = {}
data_lock = asyncio.Lock()


# -----------------------------------------------------------------
# ▼▼▼ LOGIQUE APPLICATIVE (MAP / REDUCE) ▼▼▼
# -----------------------------------------------------------------
def map_function(split_path):
    """
    CORRIGÉ : Compteur de "blocs de mots" (Word Block Count).
    Utilise re.UNICODE pour gérer l'anglais ET les blocs CJK
    sans bibliothèque externe.
    """
    logging.info(f"[MAP] Démarrage du map sur {split_path}")
    try:
        with open(split_path, "r", encoding="utf-8") as f:
            for line in f:
                # 1. \w+ (avec re.UNICODE) trouve les blocs de
                #    lettres/chiffres (ex: "home" et "学校概要")
                # 2. On applique line.lower() d'abord
                words = re.findall(r"\w+", line.lower(), re.UNICODE)

                for word in words:
                    # 3. On nettoie (strip) pour enlever les espaces/invisibles
                    word_clean = word.strip()
                    if word_clean:
                        # 4. On envoie la clé propre
                        yield (word_clean, 1)

    except FileNotFoundError:
        logging.error(f"[MAP] ERREUR: Fichier {split_path} non trouvé.")
    except Exception as e:
        logging.error(f"[MAP] ERREUR pendant le map: {e}")


def reduce_function(key, values):
    """Applique la logique de réduction (somme) pour le WordCount."""
    return (key, sum(values))


# -----------------------------------------------------------------


async def store_intermediate_data(data_object):
    """Fonction helper pour stocker les données en RAM (avec verrou)."""
    global intermediate_data, data_lock
    try:
        key, value = data_object
        async with data_lock:
            if key not in intermediate_data:
                intermediate_data[key] = []
            intermediate_data[key].append(value)
    except Exception as e:
        logging.warning(f"[SHUFFLE-STORE] Erreur stockage RAM: {e} sur {data_object}")


async def handle_shuffle_connection(reader, writer):
    """
    Tâche 1: Reçoit le shuffle et le stocke (RAM ou Disque).
    """
    addr = writer.get_extra_info("peername")

    if SHUFFLE_MODE == "ram":
        # --- MODE RAM ---
        try:
            while True:
                data_object = await protocol.receive_pickle(reader)
                if not data_object:
                    break
                await store_intermediate_data(data_object)
        except (asyncio.IncompleteReadError, ConnectionResetError):
            pass
        except Exception as e:
            logging.warning(f"[SHUFFLE-RECV-RAM] Erreur: {e} de {addr}")
        finally:
            writer.close()
            await writer.wait_closed()
    else:
        # --- MODE DISQUE ---
        shuffle_file = f"shuffle_data_{MY_SHUFFLE_PORT}.tmp"
        try:
            with open(shuffle_file, "a+", encoding="utf-8") as f:
                while True:
                    data_object = await protocol.receive_pickle(reader)
                    if not data_object:
                        break
                    try:
                        key, value = data_object
                        f.write(f"{key}\t{value}\n")
                    except (TypeError, ValueError):
                        logging.warning(
                            f"[SHUFFLE-RECV-DISK] Donnée mal formattée de {addr}"
                        )
        except (asyncio.IncompleteReadError, ConnectionResetError):
            pass
        except Exception as e:
            logging.warning(
                f"[SHUFFLE-RECV-DISK] Erreur écriture disque: {e} de {addr}"
            )
        finally:
            writer.close()
            await writer.wait_closed()


async def run_shuffle_server(host, port):
    server = await asyncio.start_server(handle_shuffle_connection, host, port)
    logging.info(f"[WORKER] Serveur de Shuffle démarré sur {host}:{port}")
    async with server:
        await server.serve_forever()


async def process_split(split_path):
    """
    Tâche 3: Applique le MAP, puis gère le SHUFFLE (Version Hybride).
    """
    logging.info(f"[WORKER-PROCESS] Traitement (Map+Shuffle) de {split_path}...")
    num_machines = len(peer_workers)
    peer_writers = {}
    map_counter = 0
    log_interval = 1_000_000
    f_local_shuffle = None

    try:
        # 1. Ouvrir connexions aux peers
        for peer_id, (host, port) in peer_workers.items():
            if peer_id == MY_WORKER_ID:
                continue
            try:
                reader, writer = await asyncio.open_connection(host, port)
                peer_writers[peer_id] = writer
            except ConnectionRefusedError:
                logging.error(
                    f"[PROCESS-SPLIT] ERREUR: Impossible de contacter le peer {peer_id}"
                )

        # 2. Ouvrir fichier local (si mode disque)
        if SHUFFLE_MODE == "disk":
            shuffle_file = f"shuffle_data_{MY_SHUFFLE_PORT}.tmp"
            f_local_shuffle = open(shuffle_file, "a", encoding="utf-8")

        # 3. Boucle Map + Shuffle
        for data_tuple in map_function(split_path):
            try:
                map_counter += 1
                if map_counter % log_interval == 0:
                    logging.info(
                        f"[MAP] ... {split_path} : {map_counter:,} mots traités..."
                    )

                key = data_tuple[0]
                # ▼▼▼ MODIFICATION PRINCIPALE ▼▼▼
                # Remplacer le hash() de Python par un hash déterministe
                hash_value = zlib.crc32(key.encode('utf-8'))
                target_index = hash_value % num_machines
                # ▲▲▲ FIN MODIFICATION ▲▲▲

                if target_index == MY_WORKER_ID:  # C'est moi
                    if SHUFFLE_MODE == "ram":
                        await store_intermediate_data(data_tuple)
                    else:
                        f_local_shuffle.write(f"{data_tuple[0]}\t{data_tuple[1]}\n")
                else:
                    writer = peer_writers.get(target_index)
                    if writer:
                        await protocol.send_pickle(writer, data_tuple)
            except Exception as e:
                logging.warning(f"[MAP-LOOP] Erreur sur {data_tuple}: {e}")

    except Exception as e:
        logging.error(f"[PROCESS-SPLIT] Erreur fatale sur {split_path}: {e}")
    finally:
        # 4. Nettoyage
        logging.info(f"[PROCESS-SPLIT] Nettoyage des connexions pour {split_path}")
        for writer in peer_writers.values():
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass
        if f_local_shuffle:
            f_local_shuffle.close()

    logging.info(
        f"[WORKER-PROCESS] Fini (Map+Shuffle): {split_path}. Total {map_counter:,} mots."
    )
    return {"type": "DONE", "payload": split_path}


async def connect_to_master(master_host, master_port):
    """
    Tâche 2: Se connecte au Master (JSON) et écoute les ordres.
    """
    # 3. CORRECTION : 'global' au début de la fonction
    global peer_workers, MY_WORKER_ID, MY_HOST_ADDRESS

    logging.info(
        f"[WORKER] Tentative de connexion au Master sur {master_host}:{master_port}"
    )
    reader, writer = None, None

    while True:
        try:
            reader, writer = await asyncio.open_connection(master_host, master_port)
            logging.info("[WORKER] Connexion au Master réussie.")
            break
        except ConnectionRefusedError:
            logging.warning("[WORKER] Master non trouvé. Nouvelle tentative...")
            await asyncio.sleep(3)
        except Exception as e:
            logging.error(f"[WORKER] Erreur de connexion inattendue: {e}")
            await asyncio.sleep(3)

    # 1. S'enregistrer
    reg_message = {"type": "REGISTER", "port": MY_SHUFFLE_PORT}
    await protocol.send_json(writer, reg_message)
    logging.info(f"[WORKER] Enregistrement... (envoi port {MY_SHUFFLE_PORT})")

    # 2. Boucle de réception des ordres
    try:
        while True:
            message = await protocol.receive_json(reader)
            if not message:
                logging.warning("[WORKER] Master déconnecté.")
                break
            msg_type = message.get("type")

            if msg_type == "SYNC":
                # La déclaration 'global' n'est plus nécessaire ici
                peer_workers = {}
                payload = message.get("payload", {})
                worker_list = payload.get("all_peers", [])
                MY_WORKER_ID = payload.get("your_id", -1)

                if MY_WORKER_ID == -1:
                    logging.error("[WORKER] ERREUR: Le Master n'a pas envoyé mon ID.")

                for w in worker_list:
                    peer_workers[w["id"]] = (w["ip"], w["port"])
                    if w["id"] == MY_WORKER_ID:
                        MY_HOST_ADDRESS = w["ip"]  # Assigne à la globale

                logging.info(
                    f"[WORKER] Sync reçue ({len(peer_workers)} pairs). Mon ID: {MY_WORKER_ID} @ {MY_HOST_ADDRESS}"
                )

            elif msg_type == "PROCESS":
                split_path = message.get("payload")
                if not split_path:
                    continue
                task = asyncio.create_task(process_split(split_path))

                async def send_done_callback(fut):
                    try:
                        result_dict = fut.result()
                        await protocol.send_json(writer, result_dict)
                    except Exception as e:
                        logging.warning(f"[WORKER] Erreur envoi 'DONE': {e}")

                task.add_done_callback(
                    lambda fut: asyncio.create_task(send_done_callback(fut))
                )

            elif msg_type == "NO_MORE_WORK":
                logging.info("[WORKER] Plus de tâches MAP. En attente de REDUCE...")

            elif msg_type == "REDUCE":
                logging.info("[WORKER] Ordre REDUCE reçu. Démarrage...")

                payload = message.get("payload", {})
                job_id = payload.get("job_id", f"job_{int(time.time())}")

                # Lit la variable globale
                output_file = f"output_{job_id}_{MY_HOST_ADDRESS}_{MY_SHUFFLE_PORT}.txt"
                pairs = []

                if SHUFFLE_MODE == "ram":
                    # --- MODE RAM (CORRIGÉ) ---
                    global intermediate_data, data_lock  # Déclarer d'abord
                    logging.info(
                        f"[REDUCE] Lecture de {len(intermediate_data)} clés depuis la RAM..."
                    )  # Lire ensuite
                    async with data_lock:
                        for key, values in intermediate_data.items():
                            for value in values:
                                pairs.append((key, value))
                    intermediate_data.clear()

                else:
                    # --- MODE DISQUE ---
                    shuffle_file = f"shuffle_data_{MY_SHUFFLE_PORT}.tmp"
                    try:
                        logging.info(
                            f"[REDUCE] Lecture du fichier shuffle {shuffle_file}..."
                        )
                        reduce_read_counter = 0
                        log_interval = 1_000_000
                        with open(shuffle_file, "r", encoding="utf-8") as f:
                            for line in f:
                                try:
                                    key, value = line.strip().split("\t")
                                    pairs.append((key, int(value)))
                                    reduce_read_counter += 1
                                    if reduce_read_counter % log_interval == 0:
                                        logging.info(
                                            f"[REDUCE] ... Lecture : {reduce_read_counter:,} paires..."
                                        )
                                except Exception:
                                    pass
                        logging.info(
                            f"[REDUCE] Lecture terminée. Total {len(pairs):,} paires."
                        )
                        os.remove(shuffle_file)
                    except FileNotFoundError:
                        logging.info(
                            f"[REDUCE] Fichier shuffle {shuffle_file} non trouvé."
                        )

                # --- Logique commune (Tri et Réduction) ---
                try:
                    logging.info(f"[REDUCE] Tri de {len(pairs):,} paires...")
                    pairs.sort(key=lambda x: x[0])
                    logging.info("[REDUCE] Tri terminé.")

                    logging.info(f"[REDUCE] Application de la réduction...")
                    with open(output_file, "w", encoding="utf-8") as f_out:
                        current_key = None
                        current_values = []
                        for key, value in pairs:
                            if key != current_key and current_key is not None:
                                result_key, result_value = reduce_function(
                                    current_key, current_values
                                )
                                f_out.write(f"{result_key}\t{result_value}\n")
                                current_values = []
                            current_key = key
                            current_values.append(value)
                        if current_key is not None:
                            result_key, result_value = reduce_function(
                                current_key, current_values
                            )
                            f_out.write(f"{result_key}\t{result_value}\n")

                    logging.info(
                        f"[WORKER] Phase REDUCE terminée. Résultat dans {output_file}"
                    )
                    await protocol.send_json(writer, {"type": "REDUCE_DONE"})
                except Exception as e:
                    logging.error(f"[REDUCE] ERREUR fatale pendant le reduce/sort: {e}")

            elif msg_type == "SHUTDOWN":
                logging.info("[WORKER] Ordre SHUTDOWN reçu. Arrêt...")
                break

    except (asyncio.CancelledError, ConnectionResetError):
        logging.warning(
            "[WORKER] Tâche Master annulée ou connexion perdue."
        )  # Typo 'WORKA' corrigée
    finally:
        if writer:
            writer.close()
            await writer.wait_closed()
        logging.info("[WORKER] Connexion au Master fermée.")  # Typo 'WORKD' corrigée


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--master-host", type=str, required=True)
    parser.add_argument("--master-port", type=int, default=8000)
    parser.add_argument("--shuffle-port", type=int, default=9001)
    parser.add_argument(
        "--shuffle-mode",
        type=str,
        default="disk",
        choices=["ram", "disk"],
        help="Mode de stockage du shuffle: 'ram' (rapide) ou 'disk' (stable).",
    )
    args = parser.parse_args()

    global MY_SHUFFLE_PORT, SHUFFLE_MODE, MY_HOST_ADDRESS
    MY_SHUFFLE_PORT = args.shuffle_port
    SHUFFLE_MODE = args.shuffle_mode

    # Le log de démarrage est géré dans le __name__ == "__main__"

    task_shuffle = asyncio.create_task(run_shuffle_server("0.0.0.0", args.shuffle_port))
    task_master_comm = asyncio.create_task(
        connect_to_master(args.master_host, args.master_port)
    )

    try:
        await asyncio.gather(task_shuffle, task_master_comm)
    except Exception as e:
        logging.error(f"[WORKER MAIN] Une tâche a échoué: {e}")
        task_shuffle.cancel()
        task_master_comm.cancel()


if __name__ == "__main__":
    # Configurer le logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        stream=sys.stdout,
    )

    # Récupérer les args pour le log de démarrage
    temp_args = sys.argv[1:]
    shuffle_mode_arg = "disk"  # default
    if "--shuffle-mode" in temp_args:
        try:
            shuffle_mode_arg = temp_args[temp_args.index("--shuffle-mode") + 1]
        except IndexError:
            pass

    logging.info(f"--- WORKER DÉMARRÉ (Mode Shuffle: {shuffle_mode_arg.upper()}) ---")

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("\n[WORKER] Arrêt manuel.")
