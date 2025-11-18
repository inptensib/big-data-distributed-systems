import asyncio
import argparse
import protocoldata701 as protocol
import os
import logging
import sys
import time
import re
import zlib
import subprocess  # ▼▼▼ AJOUT (Pour le tri externe) ▼▼▼
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor

# --- Déclarations Globales ---
peer_workers = {}
MY_SHUFFLE_PORT = 9001
MY_WORKER_ID = -1
MY_HOST_ADDRESS = "unknown_host"
SHUFFLE_MODE = "disk"
OUTPUT_DIRECTORY = "/tmp"
HEARTBEAT_INTERVAL = 5

# ▼▼▼ AJOUT (Pour corriger la race condition) ▼▼▼
SHUFFLE_SERVER_INSTANCE = None

# ▼▼▼ MODE RAM : Dictionnaire {mot: [count1, count2, ...]} ▼▼▼
intermediate_data = defaultdict(list)
data_lock = asyncio.Lock()

# ▼▼▼ CONFIGURATION BATCHING ▼▼▼
BATCH_SIZE = 50000  # ▼▼▼ MULTIPLIER PAR 5 ▼▼▼
YIELD_INTERVAL = 500000


# -----------------------------------------------------------------
# ▼▼▼ REDUCE BLOQUANT (Thread-Safe) - MODIFIÉ POUR SCALABILITÉ ▼▼▼
# -----------------------------------------------------------------
def reduce_blocking(shuffle_mode, shuffle_port, output_file, intermediate_data_copy):
    """
    Fonction BLOQUANTE qui exécute le REDUCE dans un thread.
    Utilise le tri externe (external sort) en mode disque pour éviter les OOM.
    """
    global OUTPUT_DIRECTORY
    try:
        # --- Logique de streaming (utilisée par les deux modes) ---
        def stream_reduce(input_iterator, f_out):
            logging.info("[REDUCE] Application de la réduction (en streaming)...")
            current_key = None
            current_values = []
            reduce_read_counter = 0
            log_interval = 1_000_000

            for key, value in input_iterator:
                if key != current_key and current_key is not None:
                    result_key, result_value = reduce_function(
                        current_key, current_values
                    )
                    f_out.write(f"{result_key}\t{result_value}\n")
                    current_values = []

                current_key = key
                current_values.append(value)

                reduce_read_counter += 1
                if reduce_read_counter % log_interval == 0:
                    logging.info(
                        f"[REDUCE] ... Traitement : {reduce_read_counter:,} paires..."
                    )

            # Dernière clé
            if current_key is not None:
                result_key, result_value = reduce_function(current_key, current_values)
                f_out.write(f"{result_key}\t{result_value}\n")
            logging.info(
                f"[REDUCE] Traitement streaming terminé. Total {reduce_read_counter:,} paires."
            )

        if shuffle_mode == "ram":
            # --- MODE RAM ---
            if not intermediate_data_copy:
                logging.warning("[REDUCE] Aucune donnée à réduire (RAM).")
                return True

            logging.info(
                f"[REDUCE] Lecture de {len(intermediate_data_copy)} clés depuis la RAM..."
            )
            pairs = []
            for key, values in intermediate_data_copy.items():
                for value in values:
                    pairs.append((key, value))

            logging.info(f"[REDUCE] Tri de {len(pairs):,} paires (RAM)...")
            pairs.sort(key=lambda x: x[0])
            logging.info("[REDUCE] Tri terminé.")

            with open(output_file, "w", encoding="utf-8") as f_out:
                stream_reduce(iter(pairs), f_out)

        else:
            # --- MODE DISQUE ---
            shuffle_file = f"{OUTPUT_DIRECTORY}/shuffle_data_{shuffle_port}.tmp"
            sorted_file = f"{OUTPUT_DIRECTORY}/shuffle_data_{shuffle_port}.sorted"

            try:
                # ▼▼▼ MODIFICATION (Tri externe pour éviter OOM) ▼▼▼
                logging.info(
                    f"[REDUCE] Tri externe de {shuffle_file} vers {sorted_file}..."
                )
                start_sort = time.time()
                # Utilise 'sort' (GNU coreutils).
                # LC_ALL=C est pour la performance (sort binaire/ordre des octets).
                # -k1,1 trie sur la 1ère colonne (clé).
                # -t$'\t' définit le tab comme séparateur.
                # -T spécifie un répertoire temporaire.
                sort_command = f"LC_ALL=C sort -k1,1 -t$'\t' -T {OUTPUT_DIRECTORY} {shuffle_file} -o {sorted_file}"

                logging.debug(f"[REDUCE] Commande de tri: {sort_command}")
                result = subprocess.run(
                    sort_command,
                    shell=True,  # shell=True est nécessaire pour -t$'\t'
                    check=True,
                    capture_output=True,
                    text=True,
                )
                if result.stderr:
                    logging.warning(f"[REDUCE] Sort stderr: {result.stderr}")

                logging.info(
                    f"[REDUCE] Tri externe terminé en {time.time() - start_sort:.2f}s"
                )

                # Itérateur pour lire le fichier trié ligne par ligne
                def file_iterator(sorted_file_path):
                    with open(sorted_file_path, "r", encoding="utf-8") as f_in:
                        for line in f_in:
                            try:
                                key, value = line.strip().split("\t")
                                yield (key, int(value))
                            except Exception:
                                logging.warning(
                                    f"[REDUCE] Ligne malformée ignorée: {line.strip()}"
                                )
                                pass

                with open(output_file, "w", encoding="utf-8") as f_out:
                    stream_reduce(file_iterator(sorted_file), f_out)

                # Nettoyage
                logging.info("[REDUCE] Nettoyage des fichiers temporaires.")
                os.remove(shuffle_file)
                os.remove(sorted_file)
                # ▲▲▲ FIN MODIFICATION ▲▲▲

            except FileNotFoundError:
                logging.info(
                    f"[REDUCE] Fichier shuffle {shuffle_file} non trouvé. (Mode disque)"
                )
                return True  # Pas une erreur si aucun shuffle n'a été reçu
            except subprocess.CalledProcessError as e:
                logging.error(
                    f"[REDUCE] Échec du tri externe: {e.stderr}", exc_info=True
                )
                return False
            except Exception as e:
                logging.error(
                    f"[REDUCE] Erreur pendant le reduce (disque): {e}", exc_info=True
                )
                return False

        logging.info(
            f"[REDUCE-THREAD] Phase REDUCE terminée. Résultat dans {output_file}"
        )
        return True

    except Exception as e:
        logging.error(f"[REDUCE-THREAD] ERREUR fatale: {e}", exc_info=True)
        return False


# -----------------------------------------------------------------
# ▼▼▼ LOGIQUE APPLICATIVE (MAP / REDUCE) ▼▼▼
# -----------------------------------------------------------------
def map_function(split_path):
    """
    Compteur de "blocs de mots" (Word Block Count).
    Génère des tuples (mot, 1).
    """
    logging.info(f"[MAP] Démarrage du map sur {split_path}")
    word_count = 0
    log_interval = 1_000_000

    try:
        pattern = re.compile(r"\w+", re.UNICODE)

        with open(split_path, "r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                words = pattern.findall(line.lower())
                for word in words:
                    word_clean = word.strip()
                    if word_clean:
                        word_count += 1
                        if word_count % log_interval == 0:
                            logging.info(
                                f"[MAP] ... {split_path} : {word_count:,} mots traités..."
                            )
                        # ▼▼▼ MODIFICATION (JSON envoie des listes, pas des tuples) ▼▼▼
                        # Le yield d'un tuple est ok, il sera converti en liste
                        # par le 'buffer.append()' et la sérialisation JSON.
                        yield (word_clean, 1)

        logging.info(f"[MAP] Terminé {split_path}: {word_count:,} mots au total")

    except FileNotFoundError:
        logging.error(f"[MAP] ERREUR: Fichier {split_path} non trouvé.")
    except Exception as e:
        logging.error(f"[MAP] ERREUR pendant le map: {e}")


def reduce_function(key, values):
    """Applique la logique de réduction (somme) pour le WordCount."""
    return (key, sum(values))


# -----------------------------------------------------------------
# ▼▼▼ SHUFFLE SERVER (Reçoit les batches) - MODIFIÉ (JSON) ▼▼▼
# -----------------------------------------------------------------
async def store_intermediate_data_batch(data_batch):
    """Stocke un batch de données en RAM."""
    global intermediate_data, data_lock
    try:
        async with data_lock:
            # data_batch est une liste de listes: [[k, v], [k, v]]
            for key, value in data_batch:
                intermediate_data[key].append(value)
    except Exception as e:
        logging.warning(f"[SHUFFLE-STORE] Erreur stockage RAM batch: {e}")


async def handle_shuffle_connection(reader, writer):
    """
    Reçoit le shuffle et le stocke (RAM ou Disque).
    SUPPORTE LES BATCHES (listes de listes [key, value]).
    """
    global OUTPUT_DIRECTORY
    addr = writer.get_extra_info("peername")
    shuffle_file = None
    f_out = None

    try:
        if SHUFFLE_MODE == "disk":
            shuffle_file = f"{OUTPUT_DIRECTORY}/shuffle_data_{MY_SHUFFLE_PORT}.tmp"
            f_out = open(shuffle_file, "a", encoding="utf-8")

        while True:
            # ▼▼▼ MODIFICATION (Pickle -> JSON) ▼▼▼
            # data_object sera une liste de listes: [[k, v], [k, v]]
            # ▼▼▼ MODIFICATION (Utiliser la version compressée) ▼▼▼
            data_object = await protocol.receive_compressed_json(reader)
            # ▲▲▲ FIN MODIFICATION ▲▲▲
            if not data_object:
                break

            # JSON ne nous enverra que des listes.
            if isinstance(data_object, list):
                # C'est un BATCH (ou un seul item [k,v] envoyé par JSON)

                # Vérifier si c'est bien un batch (liste de listes)
                if data_object and isinstance(data_object[0], list):
                    # C'est un BATCH
                    if SHUFFLE_MODE == "ram":
                        await store_intermediate_data_batch(data_object)
                    else:
                        for key, value in data_object:
                            f_out.write(f"{key}\t{value}\n")
                else:
                    # C'est un TUPLE UNIQUE (rétrocompatibilité)
                    # ou un batch malformé ? Tentons de le dépaqueter.
                    try:
                        key, value = data_object
                        if SHUFFLE_MODE == "ram":
                            async with data_lock:
                                intermediate_data[key].append(value)
                        else:
                            f_out.write(f"{key}\t{value}\n")
                    except Exception:
                        logging.warning(
                            f"[SHUFFLE-RECV] Donnée non-batch invalide reçue: {data_object}"
                        )

            else:
                logging.warning(
                    f"[SHUFFLE-RECV] Type de donnée inattendu (non-liste): {type(data_object)}"
                )
            # ▲▲▲

    except (asyncio.IncompleteReadError, ConnectionResetError):
        pass
    except Exception as e:
        logging.warning(f"[SHUFFLE-RECV] Erreur: {e} de {addr}")
    finally:
        if f_out:
            f_out.close()
        try:
            writer.close()
            await writer.wait_closed()
        except Exception:
            pass


async def run_shuffle_server(host, port):
    # ▼▼▼ MODIFICATION (Race Condition) ▼▼▼
    global SHUFFLE_SERVER_INSTANCE
    server = await asyncio.start_server(handle_shuffle_connection, host, port)
    SHUFFLE_SERVER_INSTANCE = server
    # ▲▲▲ FIN MODIFICATION ▲▲▲

    logging.info(f"[WORKER] Serveur de Shuffle démarré sur {host}:{port}")
    try:
        async with server:
            await server.serve_forever()
    except asyncio.CancelledError:
        logging.debug("[WORKER] Tâche run_shuffle_server annulée.")
    except Exception as e:
        # Gérer l'erreur si le serveur est fermé pendant qu'il 'serve_forever'
        logging.debug(f"[WORKER] Serveur de shuffle arrêté (cause: {e})")


# -----------------------------------------------------------------
# ▼▼▼ PROCESS SPLIT (MAP + SHUFFLE OPTIMISÉ PAR BATCHING) - MODIFIÉ (JSON) ▼▼▼
# -----------------------------------------------------------------
async def process_split(split_path):
    """
    Applique le MAP, puis gère le SHUFFLE avec BATCHING.
    Envoie les batches en JSON.
    """
    global OUTPUT_DIRECTORY
    logging.info(f"[WORKER-PROCESS] Traitement (Map+Shuffle) de {split_path}...")
    num_machines = len(peer_workers)
    peer_writers = {}
    map_counter = 0
    f_local_shuffle = None

    # ▼▼▼ BUFFERS POUR BATCHING ▼▼▼
    peer_buffers = defaultdict(list)
    local_buffer = []
    # ▲▲▲

    # ▼▼▼ DÉPLACER flush_buffer() HORS DE LA BOUCLE ▼▼▼
    async def flush_buffer(target_id, buffer):
        """Flush un buffer vers sa destination."""
        if not buffer:
            return

        start_time = time.time()

        # Le buffer est une liste de tuples (k, v).
        # send_json va le convertir en liste de listes [[k, v], [k, v]]
        data_to_send = list(buffer)

        if target_id == MY_WORKER_ID:
            # --- LOCAL ---
            if SHUFFLE_MODE == "ram":
                await store_intermediate_data_batch(data_to_send)
            else:
                for key, val in data_to_send:
                    f_local_shuffle.write(f"{key}\t{val}\n")
        else:
            # --- REMOTE ---
            writer = peer_writers.get(target_id)
            if writer:
                try:
                    # ▼▼▼ MODIFICATION (Utiliser la version compressée) ▼▼▼
                    await protocol.send_compressed_json(writer, data_to_send)
                    # ▲▲▲ FIN MODIFICATION ▲▲▲
                except Exception as e:
                    logging.error(
                        f"[SHUFFLE] Erreur envoi vers Worker {target_id}: {e}"
                    )
                    # Idéalement: gestion de l'échec (ré-essai, ou marquer le worker comme mort)
                    # Pour l'instant, on logue et on perd le batch.

        elapsed = time.time() - start_time
        if elapsed > 0.5:
            logging.warning(
                f"[SHUFFLE] Flush lent vers Worker {target_id}: {elapsed:.2f}s pour {len(data_to_send)} items"
            )

        buffer.clear()

    # ▲▲▲

    try:
        # 1. Ouvrir connexions aux peers
        for peer_id, (host, port) in peer_workers.items():
            if peer_id == MY_WORKER_ID:
                continue
            try:
                reader, writer = await asyncio.open_connection(host, port)
                peer_writers[peer_id] = writer
                logging.debug(
                    f"[PROCESS-SPLIT] Connexion établie avec Worker {peer_id}"
                )
            except ConnectionRefusedError:
                logging.error(
                    f"[PROCESS-SPLIT] Impossible de contacter le peer {peer_id} @ {host}:{port}"
                )
                # Si on ne peut pas contacter un peer, on ne peut pas continuer.
                # Gérer cette panne est complexe (le master devrait le savoir).
                # Pour l'instant, on continue, mais les données pour ce peer seront perdues.
                # Une meilleure solution serait de lever une exception ici.

        # 2. Ouvrir fichier local (si mode disque)
        if SHUFFLE_MODE == "disk":
            shuffle_file = f"{OUTPUT_DIRECTORY}/shuffle_data_{MY_SHUFFLE_PORT}.tmp"
            f_local_shuffle = open(shuffle_file, "a", encoding="utf-8")

        # 3. Boucle Map + Shuffle avec BATCHING
        for data_tuple in map_function(split_path):
            try:
                map_counter += 1

                key = data_tuple[0]
                hash_value = zlib.crc32(key.encode("utf-8"))
                target_index = hash_value % num_machines

                # Ajouter au buffer approprié
                if target_index == MY_WORKER_ID:
                    local_buffer.append(data_tuple)
                    if len(local_buffer) >= BATCH_SIZE:
                        await flush_buffer(MY_WORKER_ID, local_buffer)
                else:
                    peer_buffers[target_index].append(data_tuple)
                    if len(peer_buffers[target_index]) >= BATCH_SIZE:
                        await flush_buffer(target_index, peer_buffers[target_index])

                # Yield moins fréquent
                if map_counter % YIELD_INTERVAL == 0:
                    await asyncio.sleep(0)

            except Exception as e:
                logging.warning(f"[MAP-LOOP] Erreur sur {data_tuple}: {e}")

        # ▼▼▼ FLUSH FINAL AMÉLIORÉ AVEC STATISTIQUES ▼▼▼
        logging.info(f"[PROCESS-SPLIT] Flush final des buffers...")

        # Flush local
        if local_buffer:
            logging.info(
                f"[PROCESS-SPLIT] Flush final local: {len(local_buffer)} items restants"
            )
            await flush_buffer(MY_WORKER_ID, local_buffer)

        # Flush remote
        total_remaining = 0
        for peer_id, buffer in peer_buffers.items():
            if buffer:
                total_remaining += len(buffer)
                logging.info(
                    f"[PROCESS-SPLIT] Flush final vers Worker {peer_id}: {len(buffer)} items restants"
                )
                await flush_buffer(peer_id, buffer)

        if total_remaining > 0:
            logging.info(
                f"[PROCESS-SPLIT] Total des items dans les buffers finaux: {total_remaining:,}"
            )
        # ▲▲▲

    except Exception as e:
        logging.error(f"[PROCESS-SPLIT] Erreur fatale sur {split_path}: {e}")
    finally:
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


# -----------------------------------------------------------------
# ▼▼▼ HEARTBEAT ▼▼▼
# -----------------------------------------------------------------
async def send_heartbeat(writer):
    try:
        while True:
            await asyncio.sleep(HEARTBEAT_INTERVAL)
            await protocol.send_json(writer, {"type": "HEARTBEAT"})
    except (ConnectionError, BrokenPipeError, asyncio.CancelledError):
        logging.warning(
            "[HEARTBEAT] Connexion au Master rompue (tâche heartbeat arrêtée)."
        )


# -----------------------------------------------------------------
# ▼▼▼ CONNEXION AU MASTER - MODIFIÉ (Race Condition) ▼▼▼
# -----------------------------------------------------------------
async def connect_to_master(master_host, master_port, task_shuffle):
    """Se connecte au Master et écoute les ordres."""
    global peer_workers, MY_WORKER_ID, MY_HOST_ADDRESS, OUTPUT_DIRECTORY, intermediate_data, data_lock, SHUFFLE_SERVER_INSTANCE  # <-- AJOUT

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
            logging.error(f"[WORKER] Erreur de connexion: {e}")
            await asyncio.sleep(3)

    reg_message = {"type": "REGISTER", "port": MY_SHUFFLE_PORT}
    await protocol.send_json(writer, reg_message)
    logging.info(f"[WORKER] Enregistrement... (envoi port {MY_SHUFFLE_PORT})")

    heartbeat_task = asyncio.create_task(send_heartbeat(writer))
    executor = ThreadPoolExecutor(max_workers=1)

    try:
        while True:
            message = await protocol.receive_json(reader)
            if not message:
                logging.warning("[WORKER] Master déconnecté.")
                break

            msg_type = message.get("type")

            if msg_type == "SYNC":
                peer_workers = {}
                payload = message.get("payload", {})
                worker_list = payload.get("all_peers", [])
                MY_WORKER_ID = payload.get("your_id", -1)

                if MY_WORKER_ID == -1:
                    logging.error("[WORKER] ERREUR: Le Master n'a pas envoyé mon ID.")

                for w in worker_list:
                    peer_workers[w["id"]] = (w["ip"], w["port"])
                    if w["id"] == MY_WORKER_ID:
                        MY_HOST_ADDRESS = w["ip"]

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
                        success = await protocol.send_json(writer, result_dict)
                        if success:
                            logging.info(
                                f"[WORKER] Message 'DONE' envoyé avec succès pour {result_dict.get('payload')}"
                            )
                        else:
                            logging.error("[WORKKER] Échec de l'envoi 'DONE'")
                    except Exception as e:
                        logging.warning(f"[WORKER] Erreur envoi 'DONE': {e}")

                task.add_done_callback(
                    lambda fut: asyncio.create_task(send_done_callback(fut))
                )

            elif msg_type == "NO_MORE_WORK":
                logging.info("[WORKER] Plus de tâches MAP. En attente de REDUCE...")

            elif msg_type == "REDUCE":
                logging.info(
                    "[WORKER] Ordre REDUCE reçu. Fermeture du serveur de shuffle..."
                )

                # ▼▼▼ MODIFICATION (Correction de la Race Condition) ▼▼▼
                if SHUFFLE_SERVER_INSTANCE:
                    try:
                        SHUFFLE_SERVER_INSTANCE.close()
                        await SHUFFLE_SERVER_INSTANCE.wait_closed()
                        logging.info("[WORKER] Serveur de shuffle arrêté.")
                    except Exception as e:
                        logging.warning(
                            f"[WORKER] Erreur à la fermeture du serveur de shuffle: {e}"
                        )
                else:
                    logging.warning(
                        "[WORKER] Instance du serveur de shuffle non trouvée, impossible de fermer."
                    )

                # Attendre que la tâche du serveur se termine vraiment
                await task_shuffle
                logging.info("[WORKER] Tâche du serveur de shuffle confirmée terminée.")
                # ▲▲▲ FIN MODIFICATION ▲▲▲

                logging.info("[WORKER] Démarrage de REDUCE dans un thread...")

                payload = message.get("payload", {})
                job_id = payload.get("job_id", f"job_{int(time.time())}")
                output_file = f"{OUTPUT_DIRECTORY}/REDUCE_{job_id}_{MY_HOST_ADDRESS}_{MY_SHUFFLE_PORT}.output"

                # Copier les données RAM si nécessaire
                intermediate_data_copy = {}
                if SHUFFLE_MODE == "ram":
                    async with data_lock:
                        intermediate_data_copy = dict(intermediate_data)
                        intermediate_data.clear()

                # Exécuter REDUCE dans un thread
                loop = asyncio.get_event_loop()
                reduce_task = loop.run_in_executor(
                    executor,
                    reduce_blocking,
                    SHUFFLE_MODE,
                    MY_SHUFFLE_PORT,
                    output_file,
                    intermediate_data_copy,
                )

                def send_reduce_done_callback(future):
                    try:
                        success = future.result()
                        if success:
                            asyncio.create_task(
                                protocol.send_json(writer, {"type": "REDUCE_DONE"})
                            )
                            logging.info(
                                "[WORKER] Message 'REDUCE_DONE' envoyé avec succès"
                            )
                        else:
                            logging.error(
                                "[WORKER] REDUCE a échoué. Envoi de l'échec au Master impossible (non implémenté)."
                            )
                    except Exception as e:
                        logging.error(f"[WORKER] Erreur callback REDUCE: {e}")

                reduce_task.add_done_callback(send_reduce_done_callback)

            elif msg_type == "SHUTDOWN":
                logging.info("[WORKER] Ordre SHUTDOWN reçu. Arrêt...")
                break

    except (asyncio.CancelledError, ConnectionResetError) as e:
        logging.warning(f"[WORKER] Connexion perdue: {e}")
    except Exception as e:
        logging.error(f"[WORKER] Erreur inattendue: {e}", exc_info=True)
    finally:
        heartbeat_task.cancel()
        executor.shutdown(wait=False)
        if writer:
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass
        logging.info("[WORKER] Connexion au Master fermée.")


# -----------------------------------------------------------------
# ▼▼▼ MAIN ▼▼▼
# -----------------------------------------------------------------
async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--master-host", type=str, required=True)
    parser.add_argument("--master-port", type=int, default=8000)
    parser.add_argument("--shuffle-port", type=int, default=9001)
    parser.add_argument("--result-output", type=str, default="/tmp")
    parser.add_argument(
        "--shuffle-mode", type=str, default="ram", choices=["ram", "disk"]
    )
    parser.add_argument(
        "--batch-size", type=int, default=10000, help="Taille des batches de shuffle"
    )
    args = parser.parse_args()

    global MY_SHUFFLE_PORT, SHUFFLE_MODE, OUTPUT_DIRECTORY, BATCH_SIZE
    MY_SHUFFLE_PORT = args.shuffle_port
    SHUFFLE_MODE = args.shuffle_mode
    OUTPUT_DIRECTORY = args.result_output
    BATCH_SIZE = args.batch_size

    # La tâche du serveur de shuffle s'arrêtera quand 'server.close()'
    # sera appelé dans 'connect_to_master' (lors du REDUCE) ou ici.
    task_shuffle = asyncio.create_task(run_shuffle_server("0.0.0.0", args.shuffle_port))
    task_master_comm = asyncio.create_task(
        connect_to_master(args.master_host, args.master_port, task_shuffle)
    )

    try:
        # On attend seulement la connexion au master, qui gère maintenant
        # le cycle de vie de la tâche shuffle
        await task_master_comm
    except Exception as e:
        logging.error(f"[WORKER MAIN] Erreur: {e}")
    finally:
        task_shuffle.cancel()
        task_master_comm.cancel()
        await asyncio.gather(task_shuffle, task_master_comm, return_exceptions=True)
        logging.info("[WORKER_MAIN] Tâches principales terminées.")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        stream=sys.stdout,
    )

    temp_args = sys.argv[1:]
    shuffle_mode_arg = "ram"
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
