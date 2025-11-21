import asyncio
import argparse
import protocoldata701 as protocol
import os
import logging
import sys
import time
import regex
import unicodedata
import zlib
import functools


import uuid  # ◄-- AJOUT (pour les "runs" disque)
import heapq  # ◄-- AJOUT (pour le tri-fusion)
import glob  # ◄-- AJOUT (pour trouver les "runs")
import math  # ◄-- AJOUT (pour le Multi-Level Merge)
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

SHUFFLE_SERVER_INSTANCE = None

# OPTIMISATION RAM (Combiner)
# Stocke {mot: total_local} au lieu de {mot: [1, 1, ...]}
intermediate_data = defaultdict(int)  
data_lock = asyncio.Lock()

# CONFIGURATION BATCHING
BATCH_SIZE = 100000
YIELD_INTERVAL = 500000

# Scalabilité Disque
# Limite de sécurité pour les descripteurs de fichiers ouverts simultanément
# Doit être inférieur à 'ulimit -n' (ex: 1024, 4096)
MAX_OPEN_FILES_MERGE = 512


@functools.lru_cache(maxsize=500000)
def get_hash(key: str) -> int:
    """
    Calcule et met en cache le CRC32 d'une clé.
    Cache LRU : 500k entrées max (~10 Mo RAM).
    """
    return zlib.crc32(key.encode("utf-8"))
# -----------------------------------------------------------------
# HELPER POUR LE MULTI-LEVEL MERGE
# -----------------------------------------------------------------
def _merge_files_to_new_file(input_files: list, output_filename: str):
    """
    Prend une LISTE de chemins de fichiers triés, les fusionne,
    et écrit le résultat dans un NOUVEAU fichier.
    Gère l'ouverture/fermeture des fichiers pour ce lot.
    """

    # Itérateur interne pour parser les lignes d'un fichier
    def file_iterator(file_handle):
        for line in file_handle:
            try:
                key, value = line.strip().split("\t")
                yield (key, int(value))
            except Exception:
                pass  # Ignorer les lignes malformées

    open_files = []
    iterators = []
    try:
        # 1. Ouvrir tous les fichiers de CE LOT
        for f_path in input_files:
            try:
                f = open(f_path, "r", encoding="utf-8")
                open_files.append(f)
                iterators.append(file_iterator(f))
            except FileNotFoundError:
                logging.warning(
                    f"[REDUCE-MERGE] Fichier run {f_path} disparu pendant la pré-fusion."
                )
                pass

        if not iterators:
            logging.warning("[REDUCE-MERGE] Aucun itérateur valide pour ce lot.")
            return  # Ne crée pas de fichier de sortie

        # 2. Créer l'itérateur de fusion pour CE LOT
        merged_iterator = heapq.merge(*iterators, key=lambda x: x[0])

        # 3. Écrire le résultat dans le nouveau "super-run"
        with open(output_filename, "w", encoding="utf-8") as f_out:
            for key, value in merged_iterator:
                f_out.write(f"{key}\t{value}\n")

    except Exception as e:
        logging.error(
            f"[REDUCE-MERGE] Échec de la pré-fusion vers {output_filename}: {e}",
            exc_info=True,
        )
        # Lever l'exception pour que reduce_blocking l'attrape
        raise
    finally:
        # 4. Fermer tous les fichiers de CE LOT
        for f in open_files:
            f.close()


# -----------------------------------------------------------------
# REDUCE BLOQUANT (Thread-Safe)
# -----------------------------------------------------------------
def reduce_blocking(shuffle_mode, shuffle_port, output_file, intermediate_data_copy):
    """
    Fonction BLOQUANTE qui exécute le REDUCE dans un thread.
    Mode RAM: Utilise l'optimisation "Combiner".
    Mode Disque: Utilise "Multi-Level Sort-Merge" pour scalabilité.
    """
    global OUTPUT_DIRECTORY
    try:
        # --- Logique de streaming (commune aux deux modes) ---
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

        # --- FIN de stream_reduce ---

        if shuffle_mode == "ram":
            # --- MODE RAM (Avec Optimisation "Combiner") ---
            if not intermediate_data_copy:
                logging.warning("[REDUCE] Aucune donnée à réduire (RAM).")
                return True

            logging.info(
                f"[REDUCE] Lecture de {len(intermediate_data_copy)} clés (déjà agrégées) depuis la RAM..."
            )

            pairs = list(intermediate_data_copy.items())

            logging.info(f"[REDUCE] Tri de {len(pairs):,} paires (RAM)...")
            pairs.sort(key=lambda x: x[0])
            logging.info("[REDUCE] Tri terminé.")

            with open(output_file, "w", encoding="utf-8") as f_out:
                stream_reduce(iter(pairs), f_out)

        else:
            # --- MODE DISQUE (Avec Optimisation "Multi-Level Sort-Merge") ---

            # Itérateur helper (utilisé SEULEMENT pour la passe finale)
            def final_file_iterator(file_handle):
                for line in file_handle:
                    try:
                        key, value = line.strip().split("\t")
                        yield (key, int(value))
                    except Exception:
                        logging.warning(
                            f"[REDUCE-ITER] Ligne malformée ignorée: {line.strip()}"
                        )
                        pass

            run_files_pattern = f"{OUTPUT_DIRECTORY}/shuffle_run_{shuffle_port}_*.tmp"

            # LOGIQUE DE PRÉ-FUSION (Multi-Level Merge)

            current_level_files = glob.glob(run_files_pattern)
            files_to_cleanup = list(
                current_level_files
            )  # Garde une trace de TOUS les fichiers temp

            if not current_level_files:
                logging.info(
                    f"[REDUCE] Aucun fichier shuffle 'run' trouvé. (Mode disque)"
                )
                open(output_file, "w").close()  # Fichier vide = succès
                return True

            logging.info(
                f"[REDUCE] {len(current_level_files)} 'runs' initiaux trouvés."
            )

            level = 1
            # Tant qu'on a plus de fichiers que notre limite de descripteurs
            while len(current_level_files) > MAX_OPEN_FILES_MERGE:
                logging.info(
                    f"[REDUCE-MERGE] Niveau {level}: Fusion de {len(current_level_files)} fichiers par lots de {MAX_OPEN_FILES_MERGE}..."
                )
                next_level_files = []

                # Diviser la liste de fichiers en lots (chunks)
                num_chunks = math.ceil(len(current_level_files) / MAX_OPEN_FILES_MERGE)

                for i in range(num_chunks):
                    input_chunk = current_level_files[
                        i * MAX_OPEN_FILES_MERGE : (i + 1) * MAX_OPEN_FILES_MERGE
                    ]

                    # Créer un nom pour le nouveau "super-run"
                    output_super_run = f"{OUTPUT_DIRECTORY}/shuffle_super_run_{shuffle_port}_L{level}_C{i}.tmp"

                    logging.debug(
                        f"[REDUCE-MERGE] Création de {output_super_run} à partir de {len(input_chunk)} fichiers."
                    )
                    _merge_files_to_new_file(input_chunk, output_super_run)

                    next_level_files.append(output_super_run)

                # Les "super-runs" deviennent les entrées du prochain niveau
                current_level_files = next_level_files
                files_to_cleanup.extend(
                    next_level_files
                )  # Ajouter les super-runs au nettoyage
                level += 1


            # À ce stade, len(current_level_files) <= MAX_OPEN_FILES_MERGE
            # Nous pouvons les ouvrir tous pour la fusion finale.

            logging.info(
                f"[REDUCE] Fusion finale de {len(current_level_files)} 'runs' (Niveau {level - 1})."
            )

            open_files = []
            iterators = []
            try:
                # 1. Ouvrir tous les fichiers du DERNIER NIVEAU
                for f_path in current_level_files:
                    try:
                        f = open(f_path, "r", encoding="utf-8")
                        open_files.append(f)
                        iterators.append(final_file_iterator(f))
                    except FileNotFoundError:
                        logging.warning(f"[REDUCE-FINAL] Fichier run {f_path} disparu.")
                        pass

                if not iterators:
                    logging.warning(
                        f"[REDUCE-FINAL] Aucun fichier à fusionner à la fin."
                    )
                    open(output_file, "w").close()  # Fichier vide = succès
                    return True

                # 2. Créer l'itérateur de fusion FINAL
                merged_iterator = heapq.merge(*iterators, key=lambda x: x[0])

                # 3. Lancer la réduction sur l'itérateur fusionné
                start_reduce = time.time()
                with open(output_file, "w", encoding="utf-8") as f_out:
                    stream_reduce(merged_iterator, f_out)

                logging.info(
                    f"[REDUCE] Tri-Fusion final et Réduction terminés en {time.time() - start_reduce:.2f}s"
                )

            except Exception as e:
                logging.error(
                    f"[REDUCE] Erreur pendant le reduce (disque merge final): {e}",
                    exc_info=True,
                )
                return False
            finally:
                # 4. Nettoyage (fermer les fichiers de la passe finale)
                for f in open_files:
                    try:
                        f.close()
                    except Exception:
                        pass

                # 5. Nettoyer TOUS les fichiers temporaires (runs + super-runs)
                logging.info(
                    f"[REDUCE] Nettoyage de {len(files_to_cleanup)} fichiers temporaires..."
                )
                for f_path in files_to_cleanup:
                    try:
                        os.remove(f_path)
                    except Exception as e:
                        logging.warning(
                            f"[REDUCE-CLEANUP] Échec suppression {f_path}: {e}"
                        )

        logging.info(
            f"[REDUCE-THREAD] Phase REDUCE terminée. Résultat dans {output_file}"
        )
        return True

    except Exception as e:
        logging.error(f"[REDUCE-THREAD] ERREUR fatale: {e}", exc_info=True)
        return False


# -----------------------------------------------------------------
# LOGIQUE APPLICATIVE (MAP / REDUCE)
# -----------------------------------------------------------------
def map_function(split_path):
    """
    Compteur de mots multilingue universel (Word Count).
    Supporte tous les scripts Unicode : Latin, Cyrillique, Arabe, CJK, etc.
    Normalise les accents et les formes composites.
    """
    logging.info(f"[MAP] Démarrage du map sur {split_path}")
    word_count = 0
    log_interval = 1_000_000

    try:
        # Compilation de la regex pour la performance
        # \p{L} = toute lettre Unicode, \p{N} = tout chiffre Unicode
        pattern = regex.compile(r"[\p{L}\p{N}]+", regex.UNICODE)

        with open(split_path, "r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                # Normalisation NFKC : Convertit les caractères compatibles (ex: ﬃ -> ffi)
                # et gère les accents de manière canonique.
                line_normalized = unicodedata.normalize("NFKC", line).lower()

                words = pattern.findall(line_normalized)

                for word in words:
                    # Pas besoin de .strip() car la regex ne capture pas d'espaces
                    # Mais on garde une vérification de sécurité
                    if word:
                        word_count += 1
                        if word_count % log_interval == 0:
                            logging.info(
                                f"[MAP] ... {split_path} : {word_count:,} mots traités..."
                            )
                        yield (word, 1)

        logging.info(f"[MAP] Terminé {split_path}: {word_count:,} mots au total")

    except FileNotFoundError:
        logging.error(f"[MAP] ERREUR: Fichier {split_path} non trouvé.")
    except Exception as e:
        logging.error(f"[MAP] ERREUR pendant le map: {e}", exc_info=True)


def reduce_function(key, values):
    """Applique la logique de réduction (somme) pour le WordCount."""
    return (key, sum(values))


# -----------------------------------------------------------------
# STOCKAGE SHUFFLE (RAM) (Combiner)
# -----------------------------------------------------------------
async def store_intermediate_data_batch(data_batch):
    """
    Stocke un batch de données en RAM en agrégeant les comptes (Combiner).
    """
    global intermediate_data, data_lock
    try:
        async with data_lock:
            # data_batch est une liste de listes: [[k, v], [k, v]]
            for key, value in data_batch:
                # MODIFICATION (Optimisation Combiner)
                intermediate_data[key] += value  # ( .append(value) -> += value )
    except Exception as e:
        logging.warning(f"[SHUFFLE-STORE] Erreur stockage RAM batch: {e}")


# -----------------------------------------------------------------
# SHUFFLE SERVER (Reçoit les batches) - (Sort-Merge)
# -----------------------------------------------------------------
async def handle_shuffle_connection(reader, writer):
    """
    Reçoit le shuffle et le stocke.
    Mode RAM: Utilise le "Combiner" (agrège les totaux).
    Mode Disque: Écrit des "runs" (fichiers triés) individuels.
    """
    global OUTPUT_DIRECTORY
    addr = writer.get_extra_info("peername")

    try:
        while True:
            # Utilise la version compressée
            data_object = await protocol.receive_compressed_json(reader)
            if not data_object:
                break

            if isinstance(data_object, list):
                # Vérifier si c'est bien un batch (liste de listes)
                if data_object and isinstance(data_object[0], list):
                    # C'est un BATCH
                    if SHUFFLE_MODE == "ram":
                        await store_intermediate_data_batch(data_object)
                    else:
                        # ▼▼▼ MODIFICATION (Logique DISK optimisée - Sort-Merge) ▼▼▼

                        # 1. Trier ce batch en mémoire (rapide)
                        try:
                            data_object.sort(key=lambda x: x[0])
                        except Exception as e:
                            logging.warning(
                                f"[SHUFFLE-SORT] Batch malformé, impossible de trier: {e}"
                            )
                            continue  # Ignorer ce batch

                        # 2. Écrire ce batch trié dans un NOUVEAU fichier "run" unique
                        run_filename = f"{OUTPUT_DIRECTORY}/shuffle_run_{MY_SHUFFLE_PORT}_{uuid.uuid4().hex}.tmp"
                        try:
                            with open(run_filename, "w", encoding="utf-8") as f_run:
                                for key, value in data_object:
                                    # S'assurer que la clé ne contient pas de tabulation
                                    key_clean = str(key).replace("\t", " ")
                                    f_run.write(f"{key_clean}\t{value}\n")
                        except Exception as e:
                            logging.error(
                                f"[SHUFFLE-WRITE] Échec écriture run {run_filename}: {e}"
                            )
                else:
                    # C'est un TUPLE UNIQUE (rétrocompatibilité)
                    try:
                        key, value = data_object
                        if SHUFFLE_MODE == "ram":
                            async with data_lock:
                                intermediate_data[key] += (
                                    value  # (Optimisation Combiner)
                                )
                        else:
                            # Écrire un "run" d'une seule ligne (moins efficace, mais correct)
                            run_filename = f"{OUTPUT_DIRECTORY}/shuffle_run_{MY_SHUFFLE_PORT}_{uuid.uuid4().hex}.tmp"
                            with open(run_filename, "w", encoding="utf-8") as f_run:
                                key_clean = str(key).replace("\t", " ")
                                f_run.write(f"{key_clean}\t{value}\n")
                    except Exception:
                        logging.warning(
                            f"[SHUFFLE-RECV] Donnée non-batch invalide reçue: {data_object}"
                        )
            else:
                logging.warning(
                    f"[SHUFFLE-RECV] Type de donnée inattendu (non-liste): {type(data_object)}"
                )

    except (asyncio.IncompleteReadError, ConnectionResetError):
        pass
    except Exception as e:
        logging.warning(f"[SHUFFLE-RECV] Erreur: {e} de {addr}")
    finally:
        try:
            writer.close()
            await writer.wait_closed()
        except Exception:
            pass


async def run_shuffle_server(host, port):
    global SHUFFLE_SERVER_INSTANCE
    server = await asyncio.start_server(handle_shuffle_connection, host, port)
    SHUFFLE_SERVER_INSTANCE = server

    logging.info(f"[WORKER] Serveur de Shuffle démarré sur {host}:{port}")
    try:
        async with server:
            await server.serve_forever()
    except asyncio.CancelledError:
        logging.debug("[WORKER] Tâche run_shuffle_server annulée.")
    except Exception as e:
        logging.debug(f"[WORKK] Serveur de shuffle arrêté (cause: {e})")


# -----------------------------------------------------------------
#  PROCESS SPLIT - VERSION MONO-THREAD PURE ASYNC
# -----------------------------------------------------------------
async def process_split(split_path):
    """
    MAP en streaming pur + SHUFFLE async avec batching.
    AUCUN threading : exploit maximum du parallélisme inter-splits.
    """
    global OUTPUT_DIRECTORY
    logging.info(f"[WORKER-PROCESS] Traitement (Map+Shuffle) de {split_path}...")
    num_machines = len(peer_workers)
    if num_machines == 0:
        logging.error("[WORKER-PROCESS] Aucun peer connu (SYNC pas encore reçu ?)")
        return {"type": "DONE", "payload": split_path}

    peer_writers = {}
    map_counter = 0

    # Buffers de batching (un par destination)
    peer_buffers = defaultdict(list)
    local_buffer = []

    async def flush_buffer(target_id, buffer):
        """Flush un buffer vers sa destination."""
        if not buffer:
            return

        data_to_send = list(buffer)

        if target_id == MY_WORKER_ID:
            # LOCAL
            if SHUFFLE_MODE == "ram":
                await store_intermediate_data_batch(data_to_send)
            else:
                # Mode DISK : écrire un "run" trié
                try:
                    data_to_send.sort(key=lambda x: x[0])
                except Exception as e:
                    logging.warning(f"[SHUFFLE-SORT-LOCAL] Batch malformé: {e}")
                    buffer.clear()
                    return

                run_filename = f"{OUTPUT_DIRECTORY}/shuffle_run_{MY_SHUFFLE_PORT}_{uuid.uuid4().hex}.tmp"
                try:
                    with open(run_filename, "w", encoding="utf-8") as f_run:
                        for key, value in data_to_send:
                            key_clean = str(key).replace("\t", " ")
                            f_run.write(f"{key_clean}\t{value}\n")
                except Exception as e:
                    logging.error(
                        f"[SHUFFLE-WRITE-LOCAL] Échec écriture run {run_filename}: {e}"
                    )
        else:
            # DISTANT
            writer = peer_writers.get(target_id)
            if writer:
                try:
                    await protocol.send_compressed_json(writer, data_to_send)
                except Exception as e:
                    logging.error(
                        f"[SHUFFLE] Erreur envoi vers Worker {target_id}: {e}"
                    )

        buffer.clear()

    # 1. Ouvrir connexions aux peers
    try:
        for peer_id, (host, port) in peer_workers.items():
            if peer_id == MY_WORKER_ID:
                continue
            try:
                reader, writer = await asyncio.open_connection(host, port)
                peer_writers[peer_id] = writer
            except Exception as e:
                logging.error(
                    f"[PROCESS-SPLIT] Impossible de contacter le peer {peer_id} @ {host}:{port}: {e}"
                )
    except Exception as e:
        logging.error(
            f"[PROCESS-SPLIT] Erreur ouverture connexions: {e}", exc_info=True
        )

    # 2. MAP + SHUFFLE en streaming (MONO-THREAD)
    try:
        for key, value in map_function(split_path):
            try:
                map_counter += 1
                hash_value = get_hash(key)
                target_index = hash_value % num_machines

                if target_index == MY_WORKER_ID:
                    local_buffer.append((key, value))
                    if len(local_buffer) >= BATCH_SIZE:
                        await flush_buffer(MY_WORKER_ID, local_buffer)
                else:
                    peer_buffers[target_index].append((key, value))
                    if len(peer_buffers[target_index]) >= BATCH_SIZE:
                        await flush_buffer(target_index, peer_buffers[target_index])

                # Yield périodique pour éviter de bloquer l'event loop
                if map_counter % YIELD_INTERVAL == 0:
                    await asyncio.sleep(0)

            except Exception as e:
                logging.warning(f"[MAP-LOOP] Erreur sur ({key}, {value}): {e}")

    except Exception as e:
        logging.error(f"[PROCESS-SPLIT] Erreur fatale sur {split_path}: {e}")
        raise  # Propage l'erreur pour notifier le Master
    finally:
        # 3. Flush final de tous les buffers
        logging.info(f"[PROCESS-SPLIT] Flush final des buffers...")

        if local_buffer:
            await flush_buffer(MY_WORKER_ID, local_buffer)

        for peer_id, buffer in peer_buffers.items():
            if buffer:
                await flush_buffer(peer_id, buffer)

        # 4. Fermer toutes les connexions
        logging.info(f"[PROCESS-SPLIT] Nettoyage des connexions pour {split_path}")
        for writer in peer_writers.values():
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass

    logging.info(
        f"[WORKER-PROCESS] Fini (Map+Shuffle): {split_path}. Total {map_counter:,} mots."
    )
    return {"type": "DONE", "payload": split_path}


# -----------------------------------------------------------------
# HEARTBEAT
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
# CONNEXION AU MASTER
# -----------------------------------------------------------------
async def connect_to_master(master_host, master_port, task_shuffle):
    """Se connecte au Master et écoute les ordres."""
    global \
        peer_workers, \
        MY_WORKER_ID, \
        MY_HOST_ADDRESS, \
        OUTPUT_DIRECTORY, \
        intermediate_data, \
        data_lock, \
        SHUFFLE_SERVER_INSTANCE

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

    # Un seul thread suffit pour la tâche bloquante REDUCE
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
                        if not success:
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

                if SHUFFLE_SERVER_INSTANCE:
                    try:
                        SHUFFLE_SERVER_INSTANCE.close()
                        await SHUFFLE_SERVER_INSTANCE.wait_closed()
                        logging.info("[WORKER] Serveur de shuffle arrêté.")
                    except Exception as e:
                        logging.warning(
                            f"[WORKER] Erreur à la fermeture du serveur de shuffle: {e}"
                        )

                await task_shuffle
                logging.info("[WORKER] Tâche du serveur de shuffle confirmée terminée.")

                logging.info(
                    "[WORKER] Démarrage de REDUCE (optimisé) dans un thread..."
                )

                payload = message.get("payload", {})
                job_id = payload.get("job_id", f"job_{int(time.time())}")
                output_file = f"{OUTPUT_DIRECTORY}/REDUCE_{job_id}_{MY_HOST_ADDRESS}_{MY_SHUFFLE_PORT}.output"

                # Copier les données RAM si nécessaire (avec optimisation Combiner)
                intermediate_data_copy = {}
                if SHUFFLE_MODE == "ram":
                    async with data_lock:
                        intermediate_data_copy = dict(intermediate_data)
                        intermediate_data.clear()

                # Exécuter REDUCE (optimisé) dans un thread
                loop = asyncio.get_event_loop()
                reduce_task = loop.run_in_executor(
                    executor,
                    reduce_blocking,  # ◄-- Utilise la version optimisée finale
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
                                "[WORKER] REDUCE a échoué. Le Master devrait le détecter."
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
        executor.shutdown(wait=False, cancel_futures=True)
        if writer:
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass
        logging.info("[WORKER] Connexion au Master fermée.")


# -----------------------------------------------------------------
# ▼▼▼ MAIN (Inchangé) ▼▼▼
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

    # Créer le répertoire de sortie s'il n'existe pas
    try:
        os.makedirs(OUTPUT_DIRECTORY, exist_ok=True)
    except Exception as e:
        logging.critical(
            f"Impossible de créer le répertoire de sortie {OUTPUT_DIRECTORY}: {e}"
        )
        return

    task_shuffle = asyncio.create_task(run_shuffle_server("0.0.0.0", args.shuffle_port))
    task_master_comm = asyncio.create_task(
        connect_to_master(args.master_host, args.master_port, task_shuffle)
    )

    try:
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
