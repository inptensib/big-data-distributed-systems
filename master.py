import asyncio
import os
import argparse
import protocoldata701 as protocol  # Utilise le nom de votre module
import time
from datetime import datetime
import csv  # ⬅️ 1. IMPORTER LE MODULE CSV

# --- Déclarations Globales ---
workers = []
splits_to_process = []  # Ceci va devenir notre "file d'attente" (queue)
total_splits = 0
NUM_WORKERS_ATTENDUS = 0

# Verrou et compteurs pour les phases Map et Reduce
counter_lock = asyncio.Lock()
map_tasks_completed = 0
reduce_tasks_completed = 0
JOB_STATE = "MAPPING"  # MAPPING -> REDUCING -> FINISHED
job_start_time = 0.0  # Pour le chronomètre

RESULTS_CSV_FILE = "amdahl_results.csv"  # ⬅️ 2. DÉFINIR LE NOM DU FICHIER


# -----------------------------------------------------------------
# ▼▼▼ 3. NOUVELLE FONCTION POUR ÉCRIRE LE CSV ▼▼▼
# -----------------------------------------------------------------
def log_job_result(num_workers, num_splits, exec_time):
    """
    Ajoute une ligne de résultat au fichier CSV.
    Crée l'en-tête si le fichier n'existe pas.
    """
    try:
        # Vérifier si le fichier existe pour décider d'écrire l'en-tête
        file_exists = os.path.isfile(RESULTS_CSV_FILE)

        # Obtenir le timestamp actuel
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Ouvrir en mode 'a' (append/ajouter)
        with open(RESULTS_CSV_FILE, "a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)

            # Si le fichier est nouveau, écrire l'en-tête
            if not file_exists:
                writer.writerow(
                    ["timestamp", "num_workers", "num_splits", "execution_time_sec"]
                )

            # Écrire la ligne de données
            writer.writerow([timestamp, num_workers, num_splits, f"{exec_time:.4f}"])

        print(
            f"[MASTER] Résultat du job sauvegardé dans {RESULTS_CSV_FILE}", flush=True
        )

    except Exception as e:
        print(
            f"[MASTER] ERREUR: Impossible d'écrire dans le fichier CSV: {e}", flush=True
        )


# -----------------------------------------------------------------


async def assign_next_split(worker_writer, worker_id):
    """
    Tire le prochain split de la file d'attente et l'assigne à un worker.
    Cette fonction DOIT être appelée depuis un 'counter_lock'.
    """
    global splits_to_process

    if splits_to_process:
        # 1. Il reste du travail : prendre le prochain split
        split_path = splits_to_process.pop(0)  # .pop(0) = File (FIFO)
        print(f"[MASTER] Assignation de {split_path} à Worker {worker_id}", flush=True)
        await protocol.send_json(
            worker_writer, {"type": "PROCESS", "payload": split_path}
        )
    else:
        # 2. Il n'y a plus de travail (Map)
        print(
            f"[MASTER] Plus de splits à assigner. Worker {worker_id} est en attente (IDLE).",
            flush=True,
        )
        # Envoyer un message spécial pour dire au worker qu'il n'y a plus rien
        await protocol.send_json(worker_writer, {"type": "NO_MORE_WORK"})


async def trigger_reduce_phase():
    """
    Envoie l'ordre "REDUCE" à tous les workers.
    """
    global JOB_STATE
    JOB_STATE = "REDUCING"

    # Générer un Job ID basé sur le timestamp
    job_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    print(
        f"[MASTER] Toutes les {total_splits} tâches MAP sont terminées. Démarrage de la phase REDUCE (Job ID: {job_timestamp})...",
        flush=True,
    )

    # 3. Créer le message avec le Job ID dans le payload
    reduce_msg = {"type": "REDUCE", "payload": {"job_id": job_timestamp}}

    reduce_tasks = []
    for w in workers:
        reduce_tasks.append(protocol.send_json(w["writer"], reduce_msg))
    await asyncio.gather(*reduce_tasks, return_exceptions=True)
    print(
        f"[MASTER] Ordre REDUCE (Job ID: {job_timestamp}) envoyé aux {len(workers)} workers.",
        flush=True,
    )


async def trigger_global_shutdown():
    """
    Envoie l'ordre "SHUTDOWN" final et affiche le temps total.
    """
    global job_start_time, NUM_WORKERS_ATTENDUS, total_splits  # S'assurer que les globals sont là
    total_job_time = time.monotonic() - job_start_time
    print(
        f"\n--- TEMPS D'EXÉCUTION TOTAL (Map/Shuffle/Reduce) : {total_job_time:.4f} secondes ---",
        flush=True,
    )

    # ▼▼▼ 4. APPELER LE LOGGER CSV ▼▼▼
    log_job_result(NUM_WORKERS_ATTENDUS, total_splits, total_job_time)
    # ▲▲▲ FIN MODIFICATION ▲▲▲

    print(
        f"[MASTER] Toutes les {NUM_WORKERS_ATTENDUS} tâches REDUCE sont terminées.",
        flush=True,
    )
    global JOB_STATE
    JOB_STATE = "FINISHED"

    print("\n" + "=" * 30)
    print("[MASTER] JOB GLOBAL TERMINÉ !")
    print("=" * 30 + "\n", flush=True)

    shutdown_msg = {"type": "SHUTDOWN"}
    shutdown_tasks = []
    for w in workers:
        shutdown_tasks.append(protocol.send_json(w["writer"], shutdown_msg))
    await asyncio.gather(*shutdown_tasks, return_exceptions=True)


async def check_job_status():
    """
    Vérifie l'état du job et déclenche la prochaine phase si nécessaire.
    Doit être appelé DEPUIS un 'counter_lock'.
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
        job_start_time
    worker_info = None

    try:
        # 1. Enregistrement
        reg_message = await protocol.receive_json(reader)
        if not reg_message or reg_message.get("type") != "REGISTER":
            print(
                "[MASTER] Erreur: premier message n'est pas REGISTER. Déconnexion.",
                flush=True,
            )
            return

        addr = writer.get_extra_info("peername")
        shuffle_port = reg_message["port"]

        worker_info = {
            "id": len(workers),
            "addr": addr,
            "shuffle_addr": (addr[0], shuffle_port),
            "writer": writer,
        }
        workers.append(worker_info)
        print(
            f"[MASTER] Worker {worker_info['id']} enregistré. Écoute sur {worker_info['shuffle_addr']}",
            flush=True,
        )

        # 2. Synchronisation (si tous les workers sont là)
        if len(workers) == NUM_WORKERS_ATTENDUS:
            if job_start_time == 0.0:
                job_start_time = time.monotonic()

            print(
                "[MASTER] Tous les workers sont connectés. Synchronisation...",
                flush=True,
            )

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
                    "payload": {
                        "all_peers": worker_list_payload,
                        "your_id": w["id"],
                    },
                }
                sync_tasks.append(protocol.send_json(w["writer"], sync_message))
            await asyncio.gather(*sync_tasks)

            # 3. Assignation initiale (1 split par worker)
            print(
                f"[MASTER] Assignation de la première tâche MAP à {len(workers)} workers...",
                flush=True,
            )
            async with counter_lock:
                for w_info in workers:
                    await assign_next_split(w_info["writer"], w_info["id"])

        # 4. Boucle d'écoute des statuts
        while True:
            message = await protocol.receive_json(reader)
            if not message:
                print(
                    f"[MASTER] Worker {worker_info['id']} déconnecté (connexion fermée).",
                    flush=True,
                )
                break

            msg_type = message.get("type")

            if msg_type == "DONE":  # C'est un "MAP DONE"
                split_path = message.get("payload", "inconnu")

                async with counter_lock:
                    map_tasks_completed += 1
                    print(
                        f"[MASTER] Tâche MAP complétée: {split_path} (Total: {map_tasks_completed}/{total_splits})",
                        flush=True,
                    )
                    await assign_next_split(writer, worker_info["id"])
                    await check_job_status()

            elif msg_type == "REDUCE_DONE":
                async with counter_lock:
                    reduce_tasks_completed += 1
                    print(
                        f"[MASTER] Tâche REDUCE complétée par Worker {worker_info['id']} (Total: {reduce_tasks_completed}/{NUM_WORKERS_ATTENDUS})",
                        flush=True,
                    )
                    await check_job_status()

            else:
                pass

    except (asyncio.CancelledError, ConnectionResetError) as e:
        print(
            f"[MASTER] Connexion perdue avec {worker_info.get('id', 'Inconnu')}: {e}",
            flush=True,
        )
    finally:
        if worker_info and worker_info in workers:
            print(f"[MASTER] Nettoyage du worker {worker_info['id']}.", flush=True)
            workers.remove(worker_info)
        if writer:
            writer.close()
            await writer.wait_closed()


async def main(master_port, num_workers, splits_dir, limit_splits):
    global NUM_WORKERS_ATTENDUS, splits_to_process, total_splits
    NUM_WORKERS_ATTENDUS = num_workers

    try:
        all_files = [
            os.path.join(splits_dir, f)
            for f in os.listdir(splits_dir)
            if os.path.isfile(os.path.join(splits_dir, f))
        ]

        if limit_splits is not None and limit_splits > 0:
            splits_to_process = all_files[:limit_splits]
            print(f"[MASTER] Limite de {limit_splits} splits appliquée.", flush=True)
        else:
            splits_to_process = all_files

        splits_to_process.sort()
        total_splits = len(splits_to_process)

        print(
            f"[MASTER] Fichiers splits à traiter : {total_splits} (sur {len(all_files)} trouvés)",
            flush=True,
        )
        if not splits_to_process and num_workers > 0:
            print("[MASTER] ATTENTION: Aucun fichier split à traiter.", flush=True)

    except FileNotFoundError:
        print(
            f"[MASTER] ERREUR: Le dossier de splits '{splits_dir}' n'existe pas.",
            flush=True,
        )
        return

    server = await asyncio.start_server(handle_worker, "0.0.0.0", master_port)
    addrs = ", ".join(str(sock.getsockname()) for sock in server.sockets)
    print(f"[MASTER] Démarré. Écoute sur {addrs}...", flush=True)

    async with server:
        await server.serve_forever()


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
        help="Nombre maximum de splits à traiter (optionnel, pour le test).",
    )
    args = parser.parse_args()

    try:
        asyncio.run(main(args.port, args.workers, args.splits_dir, args.limit_splits))
    except KeyboardInterrupt:
        print("\n[MASTER] Arrêt manuel.")
