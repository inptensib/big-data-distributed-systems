#!/bin/bash

set -e

# Charger les variables d'environnement depuis le fichier .env
if [ -f ".env" ]; then
    source .env
else 
    echo "Le fichier .env est manquant"
fi

# ▼▼▼ 6. GESTION DU JOB ID ▼▼▼
JOB_ID=""
JOB_ID_FILE=".current_job_id.txt"
# ▲▲▲

MACHINES_DISPONIBLES=() 

# ▼▼▼ NOUVELLE FONCTION : Générer un Job ID ▼▼▼
generate_job_id() {
    JOB_ID="$(date +%Y%m%d_%H%M%S)"
    echo "$JOB_ID" > "$JOB_ID_FILE"
    echo "Job ID généré : $JOB_ID"
}

# ▼▼▼ NOUVELLE FONCTION : Récupérer le Job ID actuel ▼▼▼
get_current_job_id() {
    if [ -f "$JOB_ID_FILE" ]; then
        JOB_ID=$(cat "$JOB_ID_FILE")
        echo "Job ID récupéré : $JOB_ID"
    else
        echo "Erreur : Aucun Job ID trouvé. Exécutez d'abord 'start'."
        exit 1
    fi
}

# ▼▼▼ NOUVELLE FONCTION : Créer les répertoires du job ▼▼▼
create_job_directories() {
    local host="$1"
    local host_type="$2"  # "master" ou "worker"
    
    echo "  -> Création des répertoires pour $host_type sur $host"
    ssh "$host" "mkdir -p /tmp/YBT_${JOB_ID}/{logs,output}" || true
    echo "     ✓ /tmp/YBT_${JOB_ID}/logs créé"
    echo "     ✓ /tmp/YBT_${JOB_ID}/output créé"
}
# ▲▲▲


get_master_and_workers_list() {
    echo "--- Vérification de la présence de jq ---"
    if ! command -v jq &> /dev/null; then
        echo "Erreur : 'jq' n'est pas installé. Veuillez l'installer pour continuer."
        exit 1
    fi
    
    echo "--- Étape 1 : Récupération de la liste des machine disponibles ---"

    API_OUTPUT=$(curl -sL --connect-timeout 5 "$API_URL")
    CURL_EXIT_CODE=$?

    if [ $CURL_EXIT_CODE -eq 0 ]; then
        API_SERVERS_LIST=$(echo "$API_OUTPUT" | \
            jq -r '.data[] | select(.[1] == true and .[2] == 0 and .[3] == 0 and .[4] == 0) | .[0]' | \
            sed "s/^/${SSH_USER}@/; s/\$/${DNS_SUFFIX}/")
        
        if [ -n "$API_SERVERS_LIST" ]; then
            echo "Succès : Serveurs trouvés depuis l'API."
            echo "$API_SERVERS_LIST" > "$CACHE_FILE"
            readarray -t MACHINES_DISPONIBLES <<< "$API_SERVERS_LIST"
            # récupérer le premier serveur pour qu'il fasse le rôle de master et serveur nfs
            echo "--- Étape 2 : Récupération de la machine master/nfs"
            NFS_MACHINE=${MACHINES_DISPONIBLES[0]}
            MASTER_HOST=${MACHINES_DISPONIBLES[0]}
            echo "Master/NFS serveur: $MASTER_HOST"
            echo  $MASTER_HOST >    "$MASTER_NFS_SERVEUR_CACHE_FILE"        
            # supprimer le master de la liste des workers
            echo "--- Étape 3 : Récupération des workers"
            WORKERS_DISPONIBLES=("${MACHINES_DISPONIBLES[@]:1}")
            echo "$WORKERS_DISPONIBLES"
        else
            echo "Avertissement : API contactée, mais aucune machines n'est disponibles"
        fi
    else
        echo "Avertissement : Connexion à l'API ($API_URL) échouée (Code: $CURL_EXIT_CODE)."
    fi

    if [ ${#MACHINES_DISPONIBLES[@]} -eq 0 ]; then
        echo "Tentative de lecture depuis le cache ($CACHE_FILE)..."
        if [ -f "$CACHE_FILE" ]; then
            echo "Succès : Utilisation de la liste des machines depuis le cache."
            readarray -t MACHINES_DISPONIBLES < "$CACHE_FILE"
            # récupérer le premier serveur pour qu'il fasse le rôle de master et serveur nfs
            NFS_MACHINE= ${MACHINES_DISPONIBLES[0]}
            MASTER_HOST=${MACHINES_DISPONIBLES[0]}
            echo "Master/NFS serveur: $MASTER_HOST"
            # supprimer le master de la liste des workers
            WORKERS_DISPONIBLES=("${MACHINES_DISPONIBLES[@]:1}")
            echo "Workers disponibles : ${#WORKERS_DISPONIBLES[@]}"
        else
            echo "Avertissement : Fichier cache non trouvé."
            exit 1
        fi
    fi

    if [ -n "$NUM_WORKERS_TO_LAUNCH" ] && [ "$NUM_WORKERS_TO_LAUNCH" -gt 0 ] && [ "$NUM_WORKERS_TO_LAUNCH" -le "${#WORKERS_DISPONIBLES[@]}" ]; then
        WORKERS_TO_LAUNCH=("${WORKERS_DISPONIBLES[@]:0:$NUM_WORKERS_TO_LAUNCH}")
    else
        WORKERS_TO_LAUNCH=("${WORKERS_DISPONIBLES[@]}")
    fi
    NUM_WORKERS_ACTUAL=${#WORKERS_TO_LAUNCH[@]}
    
    echo "Workers sélectionnés : ${NUM_WORKERS_ACTUAL}"
    for worker in "${WORKERS_TO_LAUNCH[@]}"; do
        echo "  - $worker"
    done
    echo "${WORKERS_TO_LAUNCH[@]}" > "$WORKERS_CACHE_FILE"
}

get_stored_workers_list() {
    if [ -f "$WORKERS_CACHE_FILE" ]; then
        read -a WORKERS_TO_LAUNCH < "$WORKERS_CACHE_FILE"
        NUM_WORKERS_ACTUAL=${#WORKERS_TO_LAUNCH[@]}
    else
        echo "Erreur : Le fichier ${WORKERS_CACHE_FILE} n'existe pas."
        echo "Exécuter d'abord :: $0 retrieve-master-nfs-server-and-workers"
        exit 1
    fi
}
get_stored_master_nfs_server() {
    if [ -f "$MASTER_NFS_SERVEUR_CACHE_FILE" ]; then
        MASTER_HOST=$(cat "$MASTER_NFS_SERVEUR_CACHE_FILE")
        NFS_MACHINE=$(cat "$MASTER_NFS_SERVEUR_CACHE_FILE")
    else
        echo "Erreur : Le fichier ${MASTER_NFS_SERVEUR_CACHE_FILE} n'existe pas."
        echo "Exécuter d'abord :: $0 retrieve-master-nfs-server-and-workers"
        exit 1
    fi
}

apply_delay() {
    if [ $RATE_LIMIT -eq 1 ]; then
        echo "    [Rate Limit] Attente de ${SSH_DELAY}s..."
        sleep $SSH_DELAY
    fi
}

deploy() {
    echo "--- Déploiement du code sur le NFS via $NFS_MACHINE ---"
    rsync -avz --delete \
        --exclude="*.pyc" \
        --exclude="__pycache__" \
        --exclude="*.log" \
        --exclude="tests/" \
        --exclude="README.md" \
        --exclude="*.sh" \
        --exclude=".gitignore" \
        --exclude=".idea" \
        --exclude="*.txt" \
        --exclude="*.csv" \
        $CODE_PYTHON_DIR/*.py \
        $NFS_MACHINE:$REMOTE_PATH/
    echo "Code synchronisé."
}

start() {
    # ▼▼▼ GÉNÉRATION DU JOB ID ▼▼▼
    generate_job_id
    # ▲▲▲
    
    echo "--- Démarrage du Master (attend $NUM_WORKERS_ACTUAL workers) ---"
    echo "    [Job ID: $JOB_ID]"
    
    # ▼▼▼ CRÉATION DES RÉPERTOIRES SUR LE MASTER ▼▼▼
    create_job_directories "$MASTER_HOST" "Master"
    # ▲▲▲
    
    LIMIT_ARG=""
    if [ -n "$SPLIT_LIMIT" ]; then
        echo "    [Option] Limite de $SPLIT_LIMIT splits activée."
        LIMIT_ARG="--limit-splits $SPLIT_LIMIT"
    fi
    
    # ▼▼▼ MODIFICATION : Passer le JOB_ID au master + rediriger logs ▼▼▼
    ssh $MASTER_HOST "cd $REMOTE_PATH && \
        nohup python3 $MASTER_SCRIPT \
            --port $MASTER_PORT \
            --workers $NUM_WORKERS_ACTUAL \
            --splits-dir $DATA_PATH/ \
            --amdal-output $AMDAL_RESULT_FILE_PATH \
            $LIMIT_ARG \
            > /tmp/YBT_${JOB_ID}/logs/master.log 2>&1 &"
    # ▲▲▲
    
    echo "Master démarré sur $MASTER_HOST."
    echo "  -> Logs: /tmp/YBT_${JOB_ID}/logs/master.log"
    apply_delay
    
    echo "--- Démarrage des ${NUM_WORKERS_ACTUAL} Workers ---"
    echo "    [Mode Shuffle: $SHUFFLE_MODE]"
    current_shuffle_port=$SHUFFLE_PORT_START
    
    for worker_host in "${WORKERS_TO_LAUNCH[@]}"; do
        echo "  -> Démarrage de $worker_host sur le port shuffle $current_shuffle_port"

        # ▼▼▼ CRÉATION DES RÉPERTOIRES SUR CHAQUE WORKER ▼▼▼
        create_job_directories "$worker_host" "Worker"
        # ▲▲▲

        # ▼▼▼ MODIFICATION : Passer le JOB_ID au worker + rediriger logs ▼▼▼
        # enlever la partie user@ pour avoir juste l'IP interne
        MASTER_IP_INTERNAL=${MASTER_HOST#*@}
        ssh $worker_host "cd $REMOTE_PATH && \
            nohup python3 $WORKER_SCRIPT \
                --master-host $MASTER_IP_INTERNAL \
                --master-port $MASTER_PORT \
                --shuffle-port $current_shuffle_port \
                --shuffle-mode $SHUFFLE_MODE \
                --batch-size $BATCH_SIZE \
                --result-output /tmp/YBT_${JOB_ID}/output \
                > /tmp/YBT_${JOB_ID}/logs/worker_${current_shuffle_port}.log 2>&1 &"
        # ▲▲▲
        
        echo "     ✓ Worker démarré. Logs: /tmp/YBT_${JOB_ID}/logs/worker_${current_shuffle_port}.log"
        
        current_shuffle_port=$((current_shuffle_port + 1))
        apply_delay
    done
    echo "--- Tous les services sont démarrés ---"
    echo "Job ID: $JOB_ID"
    echo "Répertoire de travail: /tmp/YBT_${JOB_ID}/"
}

stop() {
    get_current_job_id
    echo "--- Arrêt de tous les services (Job ID: $JOB_ID) ---"
    echo "  -> Arrêt du Master sur $MASTER_HOST"
    ssh $MASTER_HOST "pkill -f 'python3 $MASTER_SCRIPT' || true" || true
    apply_delay
    
    for worker_host in "${WORKERS_TO_LAUNCH[@]}"; do
        echo "  -> Arrêt du Worker sur $worker_host"
        ssh $worker_host "pkill -f 'python3 $WORKER_SCRIPT' || true" || true
        apply_delay
    done
    echo "--- Tous les services sont arrêtés ---"
}

stop_worker_on_host() {
    local target_host="$1"
    echo "--- Arrêt du Worker sur $target_host ---"
    ssh ${SSH_USER}@$target_host${DNS_SUFFIX} "pkill -f 'python3 $WORKER_SCRIPT' || true" 
    echo "--- Worker sur $target_host arrêté ---"
}

# ▼▼▼ NOUVELLE FONCTION : Nettoyer les répertoires d'un job ▼▼▼
clean_job_directories() {
    get_current_job_id
    echo "--- Nettoyage des répertoires du Job ID: $JOB_ID ---"
    
    echo "  -> Nettoyage sur le Master"
    ssh $MASTER_HOST "rm -rf /tmp/YBT_${JOB_ID}" || true
    
    for worker_host in "${WORKERS_TO_LAUNCH[@]}"; do
        worker_ip=${worker_host#*@}
        if [ "$worker_ip" == "$MASTER_HOST" ]; then
            continue
        fi
        echo "  -> Nettoyage sur $worker_host"
        ssh $worker_host "rm -rf /tmp/YBT_${JOB_ID}" || true
    done
    
    echo "--- Nettoyage terminé ---"
}
# ▲▲▲


clean_amdal_results() {
    echo "--- Nettoyage des résultats Amdahl sur le Master ---"
    echo "  -> Nettoyage du Master ($NFS_MACHINE)"
    ssh $NFS_MACHINE "rm -f $AMDAL_RESULT_FILE_PATH" 
    echo "--- Nettoyage terminé ---"
}

clean_all() {
    clean_amdal_results
    clean_job_directories
}

logs() {
    get_current_job_id
    echo "--- Affichage des logs du Job ID: $JOB_ID (Ctrl+C pour arrêter) ---"
    trap 'kill $(jobs -p) 2>/dev/null; echo "\nArrêt du tail."; exit' INT TERM
    
    # Extraire l'IP/hostname du master pour le préfixe
    MASTER_IP=${MASTER_HOST#*@}
    
    # ▼▼▼ CORRECTION : Échapper correctement sed dans SSH ▼▼▼
    echo "Démarrage du tail sur Master ($MASTER_IP)..."
    ssh $MASTER_HOST "tail -f /tmp/YBT_${JOB_ID}/logs/master.log 2>/dev/null" 2>&1 | sed "s/^/[MASTER:${MASTER_IP}] /" &
    
    # Logs des Workers avec préfixe [WORKER:ip:port]
    current_port=$SHUFFLE_PORT_START
    for worker_host in "${WORKERS_TO_LAUNCH[@]}"; do
        WORKER_IP=${worker_host#*@}
        echo "Démarrage du tail sur Worker ($WORKER_IP:$current_port)..."
        ssh $worker_host "tail -f /tmp/YBT_${JOB_ID}/logs/worker_${current_port}.log 2>/dev/null" 2>&1 | sed "s/^/[WORKER:${WORKER_IP}:${current_port}] /" &
        current_port=$((current_port + 1))
    done
    # ▲▲▲

    wait
}

logs_worker() {
    get_current_job_id
    local worker_port="$1"
    
    if [ -z "$worker_port" ]; then
        echo "Usage: $0 logs-worker <shuffle_port>"
        echo "Exemple: $0 logs-worker 9001"
        exit 1
    fi
    
    # Trouver le worker correspondant au port
    current_port=$SHUFFLE_PORT_START
    for worker_host in "${WORKERS_TO_LAUNCH[@]}"; do
        if [ "$current_port" -eq "$worker_port" ]; then
            WORKER_IP=${worker_host#*@}
            echo "--- Logs du Worker sur $worker_host (Port: $worker_port, Job ID: $JOB_ID) ---"
            # ▼▼▼ CORRECTION : Échapper correctement sed ▼▼▼
            ssh $worker_host "tail -f /tmp/YBT_${JOB_ID}/logs/worker_${worker_port}.log 2>/dev/null" 2>&1 | sed "s/^/[WORKER:${WORKER_IP}:${worker_port}] /"
            # ▲▲▲
            return
        fi
        current_port=$((current_port + 1))
    done
    
    echo "Erreur: Aucun worker trouvé pour le port $worker_port"
    exit 1
}

collect_outputs() {
    get_current_job_id
    echo "--- Collecte des outputs du Job ID: $JOB_ID ---"
    
    LOCAL_OUTPUTS_DIR="${TEST_WORKSPACE}/collected_outputs_${JOB_ID}"
    mkdir -p "$LOCAL_OUTPUTS_DIR"
    

    current_port=$SHUFFLE_PORT_START
    for worker_host in "${WORKERS_TO_LAUNCH[@]}"; do
        worker_ip=${worker_host#*@}
        if [ "$worker_ip" == "$MASTER_HOST" ]; then
            current_port=$((current_port + 1))
            continue
        fi
        echo "  -> Collecte des outputs de $worker_host (port $current_port)"
        scp "$worker_host:/tmp/YBT_${JOB_ID}/output/REDUCE_*.output" \
            "$LOCAL_OUTPUTS_DIR/worker_${current_port}_${worker_ip}_output.txt" 2>/dev/null || echo "     Échec"
        current_port=$((current_port + 1))
    done
    
    echo "--- Outputs collectés dans $LOCAL_OUTPUTS_DIR ---"
}

show_amdal_results() {
    echo "--- Affichage des résultats Amdahl ($AMDAL_RESULT_FILE_PATH) ---"
    ssh $NFS_MACHINE "cat $AMDAL_RESULT_FILE_PATH" || echo "Aucun fichier $AMDAL_RESULT_FILE_PATH trouvé."
}

# ▼▼▼ NOUVELLE FONCTION : Test Amdahl avec pas adaptatif ▼▼▼
lauch_amdal_analysis() {
    local max_splits=$1
    local max_workers=$2
    # Déterminer le pas adaptatif
    local STEP=$((max_workers / 10))
    local START=5
    if [ $STEP -lt 1 ]; then
        STEP=1
    fi
    get_stored_master_nfs_server
    clean_amdal_results
    
    echo "=============================================="
    echo "  ANALYSE AMDAHL - Cluster Distant"
    echo "  Splits: $max_splits"
    echo "  Workers: $START à $max_workers"
    echo "=============================================="
    
    
    
    echo "  Pas d'incrémentation : $STEP"
    echo "  Points de mesure : $(seq $START $STEP $max_workers | wc -l)"
    echo ""
    
    # Boucle de tests
    for num_workers in $(seq $START $STEP $max_workers); do
        echo ""
        echo "══════════════════════════════════════════"
        echo "  TEST $num_workers/$max_workers workers"
        echo "  Job ID: $(date +%Y%m%d_%H%M%S)"
        echo "══════════════════════════════════════════"
        
        # Récupérer la liste de workers
        echo "  -> Récupération de $num_workers workers..."
        NUM_WORKERS_TO_LAUNCH=$num_workers
        get_master_and_workers_list
        
        # Déployer si premier test
        if [ $num_workers -eq 1 ]; then
            echo "  -> Déploiement du code..."
            deploy
        fi
        
        # Arrêter les services précédents
        stop 2>/dev/null || true
        sleep 3
        
        # Lancer le test
        echo "  -> Démarrage du test..."
        SPLIT_LIMIT=$max_splits
        start
        
        # Attendre la fin du job
        echo "  -> Attente de la fin du job..."
        local timeout=3600  # 1h max par test
        local elapsed=0
        local last_log=0
        
        while true; do
            # Vérifier si le job est terminé
            if ssh $MASTER_HOST "grep  'RÉSUMÉ FINAL DU JOB (SUCCÈS)' /tmp/YBT_${JOB_ID}/logs/master.log 2>/dev/null"; then
                echo "  Job terminé avec $num_workers workers"
                break
            fi
            
            sleep 10
            elapsed=$((elapsed + 10))
            
            # Log de progression toutes les 10 minutes
            if [ $((elapsed - last_log)) -ge 600 ]; then
                echo "  Attente... ${elapsed}s écoulées ($num_workers workers)"
                last_log=$elapsed
            fi
            
            if [ $elapsed -ge $timeout ]; then
                echo "  Timeout après ${timeout}s"
                stop
                break
            fi
        done
        
        # Pause entre les tests
        echo "  -> Pause de 10s avant le prochain test..."
        sleep 10
    done
    echo ""
    echo "══════════════════════════════════════════"
    echo "  ANALYSE AMDAHL TERMINÉE"
    echo "══════════════════════════════════════════"
}
# ▲▲▲



# ▼▼▼ NOUVELLE FONCTION : Test Amdahl dataset fixe ▼▼▼
lauch_amdal_analysis_fixed() {
    local num_splits=$1
    local max_workers=$2
    
    echo "=============================================="
    echo "  ANALYSE AMDAHL - Dataset fixe"
    echo "  Splits: $num_splits (fixe)"
    echo "  Workers: 5 à $max_workers"
    echo "=============================================="
    
    # Utiliser la même logique que lauch_amdal_analysis
    # mais avec num_splits constant
    lauch_amdal_analysis $num_splits $max_workers
}
# ▲▲▲

# ▼▼▼ 2. Préparation de l'Environnement ▼▼▼
prepare_environment() {
    mkdir -p "$TEST_WORKSPACE"
}
prepare_environment
# --- Point d'Entrée du Script ---
case "$1" in
    retrieve-master-nfs-server-and-workers)
        if [ -z "$2" ]; then
            echo "Usage: $0 retrieve-master-nfs-server-and-workers <number_of_workers_to_launch>"
            exit 1
        fi
        NUM_WORKERS_TO_LAUNCH="$2"
        get_master_and_workers_list
        ;;
    deploy)
        get_stored_master_nfs_server
        deploy
        ;;
    start)
        get_stored_master_nfs_server
        get_stored_workers_list
        if [ -z "$2" ]; then
            echo "Usage: $0 start <number_of_splits>"
            exit 1
        fi
        SPLIT_LIMIT="$2"
        start
        ;;
    stop)
        get_stored_master_nfs_server
        get_stored_workers_list
        stop
        ;;
    stop-worker-on-host)
        if [ -z "$2" ]; then
            echo "Usage: $0 stop-worker-on-host <worker_host>"
            exit 1
        fi
        get_stored_workers_list
        stop_worker_on_host "$2"
        ;;    
    full)
        get_stored_master_nfs_server
        get_stored_workers_list
        deploy
        start
        ;;
    full-rate-limit)
        RATE_LIMIT=1
        get_stored_master_nfs_server
        get_stored_workers_list
        deploy
        start
        ;;
    clean-all)
        get_stored_master_nfs_server
        clean_all
        ;;
    clean-amdal-results)
        get_stored_master_nfs_server
        clean_amdal_results
        ;;
    clean-job)
        get_stored_master_nfs_server
        get_stored_workers_list
        clean_job_directories
        ;;
    logs)
        get_stored_master_nfs_server
        get_stored_workers_list
        logs
        ;;
    logs-worker)
        get_stored_workers_list
        if [ -z "$2" ]; then
            echo "Usage: $0 logs-worker <shuffle_port>"
            exit 1
        fi
        logs_worker "$2"
        ;;
    collect-logs)
        get_stored_master_nfs_server
        get_stored_workers_list
        collect_logs
        ;;
    results)
        get_stored_master_nfs_server
        show_amdal_results
        ;;
    collect-outputs)
        get_stored_master_nfs_server
        get_stored_workers_list
        collect_outputs
        ;;
    launch-amdal-analysis-fixed-dataset)
        if [ -z "$2" ] || [ -z "$3" ]; then
            echo "Usage: $0 launch-amdal-analysis-fixed-dataset <num_splits> <max_workers>"
            exit 1
        fi
        lauch_amdal_analysis_fixed "$2" "$3"
        show_amdal_results
        ;;
    *)
        echo "Usage: $0 [deploy|start|stop|full|full-rate-limit|clean-amdal-results|clean-reduce-outputs|clean-job|logs|logs-worker|collect-logs|results|retrieve-master-nfs-server-and-workers|stop-worker-on-host|collect-outputs]"
        echo ""
        echo "Commandes:"
        echo "  deploy                                   - Copie les fichiers Python sur le NFS"
        echo "  start <number_of_splits>                 - Démarre le master et les workers"
        echo "  stop                                     - Arrête tous les processus"
        echo "  full <number_of_splits>                  - Déploie puis démarre"
        echo "  full-rate-limit <number_of_splits>       - Déploie puis démarre avec rate limiting"
        echo "  clean-all                                - Supprime logs, outputs et résultats Amdahl"
        echo "  clean-amdal-results                      - Supprime uniquement amdahl_results.csv"
        echo "  clean-job                                - Supprime les répertoires /tmp/YBT_* du job courant"
        echo "  logs                                     - Affiche les logs du master en temps réel"
        echo "  logs-worker <shuffle_port>               - Affiche les logs d'un worker spécifique"
        echo "  collect-logs                             - Récupère tous les logs du job courant"
        echo "  results                                  - Affiche le contenu de amdahl_results.csv"
        echo "  retrieve-master-nfs-server-and-workers <number>                - Récupère et stocke la liste des workers"
        echo "  stop-worker-on-host <worker_host>        - Arrête le worker sur l'hôte spécifié"
        echo "  collect-outputs                          - Récupère tous les outputs des workers"
        echo "  launch-amdal-analysis-fixed-dataset <num_splits> <max_workers> - Lance une analyse Amdahl sur un dataset fixe"
        exit 1
        ;;
esac