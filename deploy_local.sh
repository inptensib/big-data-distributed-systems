#!/bin/bash

# --- 1. Configuration Locale ---
set -e

MASTER_HOST="127.0.0.1"
MASTER_PORT=8000
SPLITS_DIR="tests/commoncrawl"

# Laissez vide "" pour traiter tous les splits.
SPLIT_LIMIT="6" 

# ▼▼▼ MODIFICATION PRINCIPALE ▼▼▼
# Définir ici les ports pour chaque worker local que vous voulez lancer.
# Mettez 2, 3, ou plus de ports.
WORKER_SHUFFLE_PORTS=(9001) 
# WORKER_SHUFFLE_PORTS=(9001 9002 9003 9004) # Exemple avec 4 workers

# Le nombre de workers est maintenant calculé depuis la liste
NUM_WORKERS=${#WORKER_SHUFFLE_PORTS[@]}
# ▲▲▲ FIN MODIFICATION ▲▲▲

SHUFFLE_MODE="disk" # Option par défaut, peut être modifiée si besoin

# --- 2. Fonctions ---

deploy() {
    echo "--- Déploiement local (pas d'action requise) ---"
    echo "Le code est exécuté depuis le dossier courant."
}

start() {
    # ▼▼▼ MODIFICATION ▼▼▼
    echo "--- Démarrage local (1 Master, $NUM_WORKERS Workers) ---"
    # ▲▲▲ FIN MODIFICATION ▲▲▲
    
    if [ ! -d "$SPLITS_DIR" ]; then
        echo "Erreur: Le dossier de splits '$SPLITS_DIR' n'existe pas."
        exit 1
    fi

    # Préparer l'argument optionnel --limit-splits
    LIMIT_ARG=""
    if [ -n "$SPLIT_LIMIT" ]; then
        echo "    [Option] Limite de $SPLIT_LIMIT splits activée."
        LIMIT_ARG="--limit-splits $SPLIT_LIMIT"
    fi

    # Démarrer le Master (il attend maintenant $NUM_WORKERS)
    echo "  -> Démarrage du Master sur $MASTER_HOST:$MASTER_PORT (attend $NUM_WORKERS workers)..."
    nohup python3 master.py \
        --port $MASTER_PORT \
        --workers $NUM_WORKERS \
        --splits-dir $SPLITS_DIR \
        $LIMIT_ARG \
        > master.log 2>&1 &
        
    echo "    Master PID: $!"
    sleep 1 # Laisser 1s au master pour démarrer son serveur

    # ▼▼▼ MODIFICATION PRINCIPALE ▼▼▼
    # Démarrer les Workers (en boucle)
    echo "  -> Démarrage des $NUM_WORKERS Workers..."
    
    for port in "${WORKER_SHUFFLE_PORTS[@]}"; do
        echo "    -> Lancement worker (shuffle sur $port)..."
        nohup python3 worker.py \
            --master-host $MASTER_HOST \
            --master-port $MASTER_PORT \
            --shuffle-port $port \
            --shuffle-mode $SHUFFLE_MODE \
            > worker_${port}.log 2>&1 &
        echo "       Worker PID: $!"
        sleep 0.5 # Petit délai pour laisser les processus démarrer proprement
    done
    # ▲▲▲ FIN MODIFICATION ▲▲▲

    echo "--- Services locaux démarrés ---"
    echo "Logs : master.log, worker_*.log"
    echo "Pour voir les logs en direct : tail -f *.log"
}

stop() {
    echo "--- Arrêt des services locaux ---"
    # pkill -f trouvera et tuera TOUS les processus master.py et worker.py
    pkill -f "python3 master.py" || true
    pkill -f "python3 worker.py" || true
    echo "Processus arrêtés."
}

clean() {
    echo "--- Nettoyage des fichiers logs et outputs ---"
    rm -f *.log
    rm -f output_*.txt
    echo "Logs et fichiers output supprimés."
}

# --- 3. Point d'Entrée ---
# (Inchangé)
if [ "$1" == "deploy" ]; then
    deploy
elif [ "$1" == "start" ]; then
    stop 
    start
elif [ "$1" == "stop" ]; then
    stop
elif [ "$1" == "full" ]; then
    stop
    deploy
    start
elif [ "$1" == "clean" ]; then
    clean
else
    echo "Usage: $0 [start|stop|full|clean]"
    exit 1
fi