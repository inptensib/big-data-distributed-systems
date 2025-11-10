#!/bin/bash

# --- Configuration ---
set -e

# 1. DÉTAILS DU SERVEUR NFS
NFS_MACHINE="boutrouft-25@tp-1a201-17.enst.fr"
REMOTE_PATH="/cal/exterieurs/boutrouft-25/distributed_systems"
DATA_PATH="/cal/commoncrawl"

# 2. DÉTAILS DU MASTER
MASTER_HOST="boutrouft-25@tp-1a201-17.enst.fr"
MASTER_IP_INTERNAL="tp-1a201-17.enst.fr"
MASTER_PORT=8000

# 3. DÉTAILS DES WORKERS
WORKERS=(
    "boutrouft-25@tp-1a201-18.enst.fr"
)
SHUFFLE_PORT_START=9001

# 4. RATE LIMITING
RATE_LIMIT=0 
SSH_DELAY=12

# 5. OPTIONS DE JOB
# Laissez vide "" pour traiter tous les splits.
SPLIT_LIMIT="1" 

# ▼▼▼ MODIFICATION AJOUTÉE ▼▼▼
# Mode de shuffle pour les workers:
# "ram"  = Plus rapide, utilise plus de RAM (bon pour votre serveur 15Go)
# "disk" = Plus lent, très stable (pour des données > RAM)
SHUFFLE_MODE="ram"
# ▲▲▲ FIN MODIFICATION ▲▲▲


# --- Fonctions ---

apply_delay() {
    if [ $RATE_LIMIT -eq 1 ]; then
        echo "    [Rate Limit] Attente de ${SSH_DELAY}s..."
        sleep $SSH_DELAY
    fi
}

deploy() {
    echo "--- 1. Déploiement du code sur le NFS via $NFS_MACHINE ---"
    rsync -avz --delete \
        --exclude="*.pyc" \
        --exclude="__pycache__" \
        --exclude="*.log" \
        --exclude="data/" \
        master.py \
        protocoldata701.py \
        worker.py  \
        $NFS_MACHINE:$REMOTE_PATH/
    echo "Code synchronisé."
}

start() {
    echo "--- 2. Démarrage du Master ---"
    NUM_WORKERS=${#WORKERS[@]}
    
    # Préparer l'argument --limit-splits
    LIMIT_ARG=""
    if [ -n "$SPLIT_LIMIT" ]; then
        echo "    [Option] Limite de $SPLIT_LIMIT splits activée."
        LIMIT_ARG="--limit-splits $SPLIT_LIMIT"
    fi
    
    ssh $MASTER_HOST "cd $REMOTE_PATH && \
        nohup python3 master.py \
            --port $MASTER_PORT \
            --workers $NUM_WORKERS \
            --splits-dir $DATA_PATH/ \
            $LIMIT_ARG \
            > master.log 2>&1 &"
    
    echo "Master démarré sur $MASTER_HOST. Log dans master.log."
    apply_delay
    
    echo "--- 3. Démarrage des ${NUM_WORKERS} Workers ---"
    echo "    [Mode Shuffle: $SHUFFLE_MODE]"
    current_shuffle_port=$SHUFFLE_PORT_START
    
    for worker_host in "${WORKERS[@]}"; do
        echo "  -> Démarrage de $worker_host sur le port shuffle $current_shuffle_port"
        worker_ip=${worker_host#*@}
        # ▼▼▼ MODIFICATION AJOUTÉE ▼▼▼
        # Ajout de l'argument --shuffle-mode
        ssh $worker_host "cd $REMOTE_PATH && \
            nohup python3 worker.py \
                --master-host $MASTER_IP_INTERNAL \
                --master-port $MASTER_PORT \
                --shuffle-port $current_shuffle_port \
                --shuffle-mode $SHUFFLE_MODE \
                > worker_${worker_ip}_${current_shuffle_port}_$(date +%Y%m%d%H%M%S).log 2>&1 &"
        # ▲▲▲ FIN MODIFICATION ▲▲▲
                
        apply_delay
    done
    echo "--- Tous les services sont démarrés ---"
}

stop() {
    # (Inchangé)
    echo "--- Arrêt de tous les services ---"
    echo "  -> Arrêt du Master sur $MASTER_HOST"
    ssh $MASTER_HOST "pkill -f 'python3 master.py' || true" || true
    apply_delay

    for worker_host in "${WORKERS[@]}"; do
        echo "  -> Arrêt du Worker sur $worker_host"
        ssh $worker_host "pkill -f 'python3 worker.py' || true" || true
        apply_delay
    done
    echo "--- Tous les services sont arrêtés ---"
}

clean() {
    echo "--- Nettoyage des logs et résultats sur TOUTES les machines ---"
    
    echo "  -> Nettoyage du Master ($MASTER_HOST)"
    # Supprime logs, résultats CSV, et tous les outputs
    ssh $MASTER_HOST "cd $REMOTE_PATH && rm -f *.log * output_*.txt amdahl_results.csv" || true
    apply_delay
    
    echo "--- Nettoyage terminé ---"
}

logs() {
    echo "--- Affichage des logs en direct de TOUTES les machines (Ctrl+C pour arrêter) ---"
    
    # Cette commande 'trap' intercepte le Ctrl+C (SIGINT)
    # et tue tous les processus 'ssh' en arrière-plan
    trap 'kill $(jobs -p); echo "\nArrêt du tail."; exit' INT TERM
    
    # Lancer le tail de toutes les logs en arrière-plan
    ssh $MASTER_HOST "tail -f $REMOTE_PATH/*.log" &
    
    wait
}


# --- Point d'Entrée du Script ---
# (Inchangé)
if [ "$1" == "deploy" ]; then
    deploy
elif [ "$1" == "start" ]; then
    start
elif [ "$1" == "stop" ]; then
    stop
elif [ "$1" == "full" ]; then
    deploy
    start
elif [ "$1" == "full-rate-limit" ]; then
    RATE_LIMIT=1
    deploy
    startelif [ "$1" == "clean" ]; then
    clean
elif [ "$1" == "logs" ]; then
    logs
else
    echo "Usage: $0 [deploy|start|stop|full|full-rate-limit|clean|logs]"
    echo "  deploy:            Copie les fichiers sur le NFS."
    echo "  start:             Démarre le master et les workers."
    echo "  stop:              Arrête tous les processus."
    echo "  full:              Fait 'deploy' puis 'start'."
    echo "  full-rate-limit:   Fait 'full' avec le délai (machine source)."
    echo "  clean:             Supprime tous les logs et outputs sur les machines."
    echo "  logs:              Affiche les logs de toutes les machines en direct."
    exit 1
fi