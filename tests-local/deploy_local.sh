#!/bin/bash

set -e



# Charger les variables d'environnement depuis le fichier .env
if [ -f ".env" ]; then
    source .env
else 
    echo "Le fichier .env est manquant"
fi

SPLITS_DIR="${SPLITS_TARGET_DIR}/${SPLITS_DATASET_TYPE}"
# Générer un Job ID 
generate_job_id() {
    JOB_ID="$(date +%Y%m%d_%H%M%S)"
    echo "$JOB_ID" > "$JOB_ID_FILE"
    echo "Job ID généré : $JOB_ID"
}

# Récupérer le Job ID actuel 
get_current_job_id() {
    if [ -f "$JOB_ID_FILE" ]; then
        JOB_ID=$(cat "$JOB_ID_FILE")
        echo "Job ID récupéré : $JOB_ID"
    else
        echo "Erreur : Aucun Job ID trouvé. Exécutez d'abord 'start'."
        exit 1
    fi
}

# Créer les répertoires du job
create_job_directories() {
    echo "  -> Création des répertoires "
   mkdir -p ${TEST_WORKSPACE}/${JOB_ID}/{logs,output}
   echo "     ${TEST_WORKSPACE}/${JOB_ID}/logs créé"
   echo "     ${TEST_WORKSPACE}/${JOB_ID}/output créé"
}
# 
deploy() {
    cp ${CODE_PYTHON_DIR}/*.py ${TEST_WORKSPACE}/
    echo "Les fichiers Python ont été copiés dans ${TEST_WORKSPACE}/"
}

start() {
    echo "--- Démarrage local (1 Master, $NUM_WORKERS Workers) ---"
    generate_job_id
    create_job_directories
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
    nohup python3 ${TEST_WORKSPACE}/$MASTER_SCRIPT \
        --port $MASTER_PORT \
        --workers $NUM_WORKERS \
        --splits-dir $SPLITS_DIR \
        --amdal-output $AMDAL_RESULT_FILE_PATH \
        $LIMIT_ARG \
        > ${TEST_WORKSPACE}/${JOB_ID}/logs/master.log 2>&1 &
        
    echo "    Master PID: $!"
    sleep 1 # Laisser 1s au master pour démarrer son serveur

    # Démarrer les Workers (en boucle)
    echo "  -> Démarrage des $NUM_WORKERS Workers..."
    

    for i in $(seq 0 $((NUM_WORKERS - 1))); do
        echo "    -> Lancement worker (shuffle sur $((WORKERS_INITIAL_PORT + i + 1)))..."
        nohup python3 ${TEST_WORKSPACE}/$WORKER_SCRIPT \
            --master-host $MASTER_HOST \
            --master-port $MASTER_PORT \
            --shuffle-port $((WORKERS_INITIAL_PORT + i + 1)) \
            --shuffle-mode $SHUFFLE_MODE \
            --batch-size $SHUFFLE_BATCH_SIZE \
            --result-output ${TEST_WORKSPACE}/${JOB_ID}/output \
                > ${TEST_WORKSPACE}/${JOB_ID}/logs/worker_$((WORKERS_INITIAL_PORT + i + 1)).log 2>&1 &
        echo "       Worker PID: $!"
        sleep 1 # Délai pour laisser les processus démarrer proprement
    done

    echo "--- Services démarrés ---"
    echo "Logs : ${TEST_WORKSPACE}/${JOB_ID}/logs/master.log, ${TEST_WORKSPACE}/${JOB_ID}/logs/worker_*.log"
    echo "Pour voir les logs en direct : tail -f ${TEST_WORKSPACE}/${JOB_ID}/logs/*.log"
}

stop() {
    echo "--- Arrêt des services  ---"
    # pkill -f trouvera et tuera TOUS les processus master.py et worker.py
    pkill -f "python3 ${TEST_WORKSPACE}/$MASTER_SCRIPT" || true
    pkill -f "python3 ${TEST_WORKSPACE}/$WORKER_SCRIPT" || true
    echo "Processus arrêtés."
}

stop_worker_on_port() {
    local target_port="$1"
    echo "--- Arrêt du Worker sur le port $target_port ---"
    
    # Trouver le PID du processus qui utilise ce port en argument
    PID=$(ps aux | grep ${TEST_WORKSPACE}/$WORKER_SCRIPT | grep "shuffle-port $target_port" | grep -v grep | awk '{print $2}')
    
    if [ -n "$PID" ]; then
        echo "PID trouvé: $PID"
        kill -9 $PID
        echo "Worker sur le port $target_port arrêté"
    else
        echo "Aucun worker trouvé sur le port $target_port"
    fi
}


logs() {
    echo "--- Affichage des logs (Master et Workers) en direct ---"
    get_current_job_id
    tail -f ${TEST_WORKSPACE}/${JOB_ID}/logs/master.log ${TEST_WORKSPACE}/${JOB_ID}/logs/worker_*.log
}

clean_amdal_results() {
    echo "--- Nettoyage des résultats Amdahl ---"
    rm -f $AMDAL_RESULT_FILE_PATH
    echo "Fichier $AMDAL_RESULT_FILE_PATH supprimé."
}


show_amdal_results() {
    if [ -f "$AMDAL_RESULT_FILE_PATH" ]; then
        echo "--- Contenu du fichier $AMDAL_RESULT_FILE_PATH ---"
        cat "$AMDAL_RESULT_FILE_PATH"
    else
        echo "Le fichier $AMDAL_RESULT_FILE_PATH n'existe pas."
    fi
}

# --- 2. Préparation de l'Environnement ---
prepare_environment() {
    mkdir -p "$TEST_WORKSPACE"
}
prepare_environment
# --- 3. Point d'Entrée ---
if [ "$1" == "deploy" ]; then
    deploy
elif [ "$1" == "start" ]; then
    if [ -z "$2" ] || [ -z "$3" ]; then
        echo "Usage: $0 start <number_of_splits> <number_of_workers>"
        exit 1
    fi
    SPLIT_LIMIT="$2"
    NUM_WORKERS="$3"
    stop
    start
elif [ "$1" == "stop" ]; then
    stop
elif [ "$1" == "full" ]; then
    if [ -z "$2" ] || [ -z "$3" ]; then
        echo "Usage: $0 start <number_of_splits> <number_of_workers>"
        exit 1
    fi
    SPLIT_LIMIT="$2"
    NUM_WORKERS="$3"
    stop
    deploy
    start
    logs
elif [ "$1" == "clean-amdal-results" ]; then
    clean_amdal_results
elif [ "$1" == "logs" ]; then
    logs
elif [ "$1" == "results" ]; then
    show_amdal_results
elif [ "$1" == "stop-worker-on-port" ]; then
    if [ -z "$2" ]; then
        echo "Usage: $0 stop-worker-on-port <worker_shuffle_port>"
        exit 1
    fi
    stop_worker_on_port "$2"
else
    echo "Usage: $0 {deploy|start|stop|full|logs|clean-amdal-results|results|stop-worker-on-port <port>}"
    echo "  deploy                 : Déployer les fichiers Python dans le workspace local"
    echo "  start  <number_of_splits> : Démarrer le Master et les Workers locaux"
    echo "  stop                   : Arrêter le Master et les Workers locaux"
    echo "  full  <number_of_splits>                 : Arrêter, déployer , démarrer à nouveau et affichers les logs"
    echo "  logs                   : Afficher les logs du Master et des Workers en direct"
    echo "  clean-amdal-results    : Supprimer le fichier des résultats Amdahl"
    echo "  results                : Afficher le contenu du fichier des résultats Amdahl"
    echo "  stop-worker-on-port <port> : Arrêter le Worker utilisant le port de shuffle spécifié"
    exit 1
fi