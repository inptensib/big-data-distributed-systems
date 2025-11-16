import asyncio
import os
import argparse
import protocoldata701 as protocol  # Utilise le nom de votre module
import time
from datetime import datetime
import csv
import logging
import sys

# --- Déclarations Globales ---
workers = []
splits_to_process = []
splits_in_progress = {}
total_splits = 0
NUM_WORKERS_ATTENDUS = 0

counter_lock = asyncio.Lock()
map_tasks_completed = 0
reduce_tasks_completed = 0
JOB_STATE = "MAPPING"
job_start_time = 0.0

RESULTS_CSV_FILE = "amdahl_results.csv"
total_data_size_mb = 0.0

HEALTH_CHECK_INTERVAL = 10
WORKER_TIMEOUT = 30


def log_job_result(num_workers, num_splits, total_data_size_mb, exec_time):
    """
    Ajoute une ligne de résultat au fichier CSV.
    """
    try:
        file_exists = os.path.isfile(RESULTS_CSV_FILE)
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        with open(RESULTS_CSV_FILE, "a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            if not file_exists:
                writer.writerow(
                    [
                        "timestamp",
                        "num_workers",
                        "num_splits",
                        "total_data_size_mb",
                        "execution_time_sec",
                    ]
                )
            writer.writerow(
                [
                    timestamp,
                    num_workers,
                    num_splits,
                    f"{total_data_size_mb:.2f}",
                    f"{exec_time:.4f}",
                ]
            )
        logging.info(f"[MASTER] Résultat du job sauvegardé dans {RESULTS_CSV_FILE}")

    except Exception as e:
        logging.error(f"[MASTER] ERREUR: Impossible d'écrire dans le fichier CSV: {e}")


async def assign_next_split(worker_writer, worker_id):
    """
    Tire le prochain split de la file d'attente, l'assigne,
    ET le marque comme "en cours".
    """
    global splits_to_process, splits_in_progress

    if splits_to_process:
        split_path = splits_to_process.pop(0)
        splits_in_progress[split_path] = worker_id
        logging.info(f"[MASTER] Assignation de {split_path} à Worker {worker_id}")

        success = await protocol.send_json(
            worker_writer, {"type": "PROCESS", "payload": split_path}
        )
        if not success:
            logging.error(
                f"[MASTER] ÉCHEC d'envoi à Worker {worker_id}. Ré-ajout du split."
            )
            splits_to_process.insert(0, split_path)
            if split_path in splits_in_progress:
                del splits_in_progress[split_path]
    else:
        logging.info(
            f"[MASTER] Plus de splits à assigner. Worker {worker_id} est en attente (IDLE)."
        )
        await protocol.send_json(worker_writer, {"type": "NO_MORE_WORK"})


async def trigger_reduce_phase():
    """
    Envoie l'ordre "REDUCE" à tous les workers.
    """
    global JOB_STATE, splits_in_progress
    JOB_STATE = "REDUCING"

    if splits_in_progress:
        logging.warning(
            f"[MASTER] ATTENTION: {len(splits_in_progress)} splits encore en cours lors du déclenchement de REDUCE!"
        )

    job_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    logging.info(
        f"[MASTER] Toutes les {total_splits} tâches MAP sont terminées. Démarrage de la phase REDUCE (Job ID: {job_timestamp})..."
    )

    reduce_msg = {"type": "REDUCE", "payload": {"job_id": job_timestamp}}

    reduce_tasks = []
    for w in workers:
        reduce_tasks.append(protocol.send_json(w["writer"], reduce_msg))
    await asyncio.gather(*reduce_tasks, return_exceptions=True)
    logging.info(
        f"[MASTER] Ordre REDUCE (Job ID: {job_timestamp}) envoyé aux {len(workers)} workers."
    )


# -----------------------------------------------------------------
# ▼▼▼ FONCTION MODIFIÉE ▼▼▼
# -----------------------------------------------------------------
async def trigger_global_shutdown():
    """
    Envoie l'ordre "SHUTDOWN" final, affiche le résumé et logue le résultat.
    """
    global job_start_time, NUM_WORKERS_ATTENDUS, total_splits, total_data_size_mb

    # Calcul des temps
    total_job_time_sec = time.monotonic() - job_start_time
    total_job_time_min = total_job_time_sec / 60.0

    # ▼▼▼ NOUVEAU BLOC DE RÉSUMÉ ▼▼▼
    logging.info("\n" + "=" * 40)
    logging.info("---  RÉSUMÉ FINAL DU JOB  ---")
    logging.info(f"    Nombre total de splits traités : {total_splits}")
    logging.info(f"    Nombre de workers utilisés    : {NUM_WORKERS_ATTENDUS}")
    logging.info(
        f"    Temps d'exécution total : {total_job_time_min:.2f} minutes ({total_job_time_sec:.2f} secondes)"
    )
    logging.info("=" * 40 + "\n")
    # ▲▲▲ FIN DU NOUVEAU BLOC ▲▲▲

    # Logguer dans le CSV (utilise les secondes pour la précision)
    log_job_result(
        NUM_WORKERS_ATTENDUS,
        total_splits,
        total_data_size_mb,
        total_job_time_sec,
    )

    logging.info(
        f"[MASTER] Toutes les {NUM_WORKERS_ATTENDUS} tâches REDUCE sont terminées."
    )
    global JOB_STATE
    JOB_STATE = "FINISHED"

    logging.info("\n" + "=" * 30 + "\n [MASTER] JOB GLOBAL TERMINÉ ! \n" + "=" * 30)

    shutdown_msg = {"type": "SHUTDOWN"}
    shutdown_tasks = []
    for w in workers:
        shutdown_tasks.append(protocol.send_json(w["writer"], shutdown_msg))
    await asyncio.gather(*shutdown_tasks, return_exceptions=True)


# -----------------------------------------------------------------
# ▲▲▲ FIN DE LA MODIFICATION ▲▲▲
# -----------------------------------------------------------------


async def check_job_status():
    """
    Vérifie l'état du job et déclenche la prochaine phase si nécessaire.
    """
    global \
        JOB_STATE, \
        map_tasks_completed, \
        reduce_tasks_completed, \
        total_splits, \
        NUM_WORKERS_ATTENDUS

    if JOB_STATE == "MAPPING" and map_tasks_completed == total_splits:
        await trigger_reduce_phase()
    elif JOB_STATE == "REDUCING" and reduce_tasks_completed == NUM_WORKERS_ATTENDUS:
        await trigger_global_shutdown()


async def handle_worker_death(worker_info, reason="inconnue"):
    """
    Gère la panne d'un worker.
    """
    global workers, splits_in_progress, splits_to_process, NUM_WORKERS_ATTENDUS

    if worker_info not in workers:
        return  # Déjà géré

    worker_id = worker_info["id"]
    logging.warning(
        f"[MASTER] DÉTECTION DE PANNE: Worker {worker_id} est mort (Raison: {reason})."
    )

    workers.remove(worker_info)
    NUM_WORKERS_ATTENDUS = len(workers)
    try:
        worker_info["writer"].close()
        await worker_info["writer"].wait_closed()
    except Exception:
        pass

    tasks_to_requeue = []
    for split_path, assigned_worker_id in list(splits_in_progress.items()):
        if assigned_worker_id == worker_id:
            tasks_to_requeue.append(split_path)

    if not tasks_to_requeue:
        logging.info(f"[MASTER] Le worker {worker_id} n'avait pas de tâche en cours.")
        return

    logging.info(
        f"[MASTER] Ré-assignation de {len(tasks_to_requeue)} tâches du Worker {worker_id}..."
    )
    async with counter_lock:
        for split_path in tasks_to_requeue:
            del splits_in_progress[split_path]
            splits_to_process.insert(0, split_path)
            logging.info(f"[MASTER] Split {split_path} remis dans la queue.")


async def handle_worker(reader, writer):
    """
    Gère la connexion pour UN SEUL worker.
    """
    global \
        workers, \
        map_tasks_completed, \
        reduce_tasks_completed, \
        counter_lock, \
        total_splits, \
        NUM_WORKERS_ATTENDUS, \
        job_start_time, \
        splits_in_progress

    worker_info = None

    try:
        reg_message = await protocol.receive_json(reader)
        if not reg_message or reg_message.get("type") != "REGISTER":
            logging.error(
                "[MASTER] Erreur: premier message n'est pas REGISTER. Déconnexion."
            )
            return

        addr = writer.get_extra_info("peername")
        shuffle_port = reg_message["port"]

        worker_info = {
            "id": len(workers),
            "addr": addr,
            "shuffle_addr": (addr[0], shuffle_port),
            "writer": writer,
            "last_heartbeat": time.monotonic(),
        }
        workers.append(worker_info)
        logging.info(
            f"[MASTER] Worker {worker_info['id']} enregistré. Écoute sur {worker_info['shuffle_addr']}"
        )

        if len(workers) == NUM_WORKERS_ATTENDUS:
            if job_start_time == 0.0:
                job_start_time = time.monotonic()

            logging.info("[MASTER] Tous les workers sont connectés. Synchronisation...")
            worker_list_payload = [
                {
                    "id": w["id"],
                    "ip": w["shuffle_addr"][0],
                    "port": w["shuffle_addr"][1],
                }
                for w in workers
            ]
            sync_tasks = []
            for w in workers:
                sync_message = {
                    "type": "SYNC",
                    "payload": {"all_peers": worker_list_payload, "your_id": w["id"]},
                }
                sync_tasks.append(protocol.send_json(w["writer"], sync_message))
            await asyncio.gather(*sync_tasks)

            logging.info(
                f"[MASTER] Assignation de la première tâche MAP à {len(workers)} workers..."
            )
            async with counter_lock:
                assignment_tasks = []
                for w_info in workers:
                    task = assign_next_split(w_info["writer"], w_info["id"])
                    assignment_tasks.append(task)
                await asyncio.gather(*assignment_tasks)
            logging.info("[MASTER] Toutes les assignations initiales sont envoyées.")

        while True:
            message = await protocol.receive_json(reader)
            if not message:
                raise ConnectionResetError("Le worker a fermé la connexion proprement.")

            if worker_info:
                worker_info["last_heartbeat"] = time.monotonic()

            msg_type = message.get("type")

            if msg_type == "DONE":
                split_path = message.get("payload", "inconnu")
                async with counter_lock:
                    if (
                        split_path in splits_in_progress
                        and splits_in_progress.get(split_path) == worker_info["id"]
                    ):
                        del splits_in_progress[split_path]
                        map_tasks_completed += 1
                        logging.info(
                            f"[MASTER] Tâche MAP complétée: {split_path} (Total: {map_tasks_completed}/{total_splits})"
                        )
                    else:
                        logging.warning(
                            f"[MASTER] ATTENTION: DONE reçu pour {split_path} mais non suivi (tâche ré-assignée ou dupliquée ?)"
                        )
                        continue

                    await assign_next_split(writer, worker_info["id"])
                    await check_job_status()

            elif msg_type == "REDUCE_DONE":
                async with counter_lock:
                    reduce_tasks_completed += 1
                    logging.info(
                        f"[MASTER] Tâche REDUCE complétée par Worker {worker_info['id']} (Total: {reduce_tasks_completed}/{NUM_WORKERS_ATTENDUS})"
                    )
                    await check_job_status()

            elif msg_type == "HEARTBEAT":
                pass

    except (asyncio.CancelledError, ConnectionResetError, BrokenPipeError) as e:
        logging.warning(
            f"[MASTER] Connexion perdue avec Worker {worker_info.get('id', 'Inconnu')}: {e}"
        )
        if worker_info:
            async with counter_lock:
                await handle_worker_death(worker_info, reason=str(e))
    except Exception as e:
        logging.error(
            f"[MASTER] Erreur inattendue avec Worker {worker_info.get('id', 'Inconnu')}: {e}",
            exc_info=True,
        )
    finally:
        if worker_info and worker_info in workers:
            logging.info(f"[MASTER] Nettoyage final du worker {worker_info['id']}.")
            async with counter_lock:
                if worker_info in workers:
                    workers.remove(worker_info)
        if writer:
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass


async def health_check_loop():
    """
    Tourne en arrière-plan et vérifie les workers zombies.
    """
    while True:
        await asyncio.sleep(HEALTH_CHECK_INTERVAL)

        async with counter_lock:
            workers_to_check = workers[:]
        now = time.monotonic()

        for worker_info in workers_to_check:
            if "last_heartbeat" in worker_info and (
                now - worker_info["last_heartbeat"] > WORKER_TIMEOUT
            ):
                logging.warning(
                    f"[HEALTH-CHECK] Worker {worker_info['id']} est TIMEOUT (dernier heartbeat il y a {now - worker_info['last_heartbeat']:.0f}s)."
                )
                async with counter_lock:
                    await handle_worker_death(worker_info, reason="Timeout (zombie)")


async def main(master_port, num_workers, splits_dir, limit_splits):
    global \
        NUM_WORKERS_ATTENDUS, \
        splits_to_process, \
        total_splits, \
        total_data_size_mb, \
        splits_in_progress
    NUM_WORKERS_ATTENDUS = num_workers
    splits_in_progress = {}

    try:
        all_files = [
            os.path.join(splits_dir, f)
            for f in os.listdir(splits_dir)
            if os.path.isfile(os.path.join(splits_dir, f))
        ]
        all_files.sort()

        if limit_splits is not None and limit_splits > 0:
            splits_to_process = all_files[:limit_splits]
            logging.info(f"[MASTER] Limite de {limit_splits} splits appliquée.")
        else:
            splits_to_process = all_files

        total_splits = len(splits_to_process)

        total_bytes = 0
        for f_path in splits_to_process:
            try:
                total_bytes += os.path.getsize(f_path)
            except FileNotFoundError:
                logging.error(
                    f"[MASTER] ERREUR: Fichier split {f_path} non trouvé (calcul taille)."
                )

        total_data_size_mb = total_bytes / (1024 * 1024)

        logging.info(
            f"[MASTER] Fichiers splits à traiter : {total_splits} (sur {len(all_files)} trouvés). Taille totale: {total_data_size_mb:.4f} Mo"
        )
        if not splits_to_process and num_workers > 0:
            logging.warning("[MASTER] ATTENTION: Aucun fichier split à traiter.")

    except FileNotFoundError:
        logging.error(
            f"[MASTER] ERREUR: Le dossier de splits '{splits_dir}' n'existe pas."
        )
        return

    server = await asyncio.start_server(handle_worker, "0.0.0.0", master_port)
    addrs = ", ".join(str(sock.getsockname()) for sock in server.sockets)
    logging.info(f"[MASTER] Démarré. Écoute sur {addrs}...")

    health_task = asyncio.create_task(health_check_loop())

    try:
        async with server:
            await server.serve_forever()
    finally:
        health_task.cancel()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--port", type=int, default=8000, help="Port d'écoute du Master"
    )
    parser.add_argument(
        "--workers", type=int, required=True, help="Nombre de workers à attendre"
    )
    parser.add_argument(
        "--splits-dir",
        type=str,
        required=True,
        help="Dossier (NFS) contenant les splits",
    )
    parser.add_argument(
        "--limit-splits",
        type=int,
        default=None,
        help="Nombre maximum de splits à traiter.",
    )
    parser.add_argument(
        "--amdal-output",
        type=str,
        default=None,
        help="Fichier CSV pour les résultats d'Amdahl.",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        stream=sys.stdout,
    )

    try:
        if args.amdal_output:
            RESULTS_CSV_FILE = args.amdal_output
        asyncio.run(main(args.port, args.workers, args.splits_dir, args.limit_splits))
    except KeyboardInterrupt:
        logging.info("\n[MASTER] Arrêt manuel.")
