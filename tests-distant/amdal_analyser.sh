#!/bin/bash

set -e  # Arrêter en cas d'erreur

# ============================================================
#   ANALYSE AMDAHL - SCRIPT DE LANCEMENT
#   Charge les résultats et génère les courbes
# ============================================================

# Couleurs
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

print_header() {
    echo -e "${BLUE}============================================================${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}============================================================${NC}"
}

print_success() {
    echo -e "${GREEN}✓ $1${NC}"
}

print_error() {
    echo -e "${RED}✗ $1${NC}"
}

print_info() {
    echo -e "${BLUE}→ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠ $1${NC}"
}

# ============================================================
# USAGE
# ============================================================
usage() {
    echo "Usage: $0 [OPTIONS] [CSV_PATH]"
    echo ""
    echo "Arguments:"
    echo "  CSV_PATH                    Chemin vers le fichier amdahl_results.csv"
    echo "                              (optionnel, utilise AMDAL_RESULT_FILE_PATH de .env par défaut)"
    echo ""
    echo "Options:"
    echo "  -h, --help                  Afficher cette aide"
    echo "  --no-show                   Ne pas afficher les graphiques (sauvegarder seulement)"
    echo "  --output-dir DIR            Répertoire de sortie (défaut: ./amdahl_analysis)"
    echo ""
    echo "Exemples:"
    echo "  $0                                              # Utilise .env"
    echo "  $0 /path/to/results.csv                         # Chemin explicite"
    echo "  $0 --output-dir ./mon_analyse results.csv       # Sortie custom"
    echo "  $0 --no-show /remote/results.csv                # Pas d'affichage graphique"
    exit 0
}

# ============================================================
# PARSE ARGUMENTS
# ============================================================
CSV_PATH=""
OUTPUT_DIR=""
NO_SHOW_FLAG=""

while [[ $# -gt 0 ]]; do
    case $1 in
        -h|--help)
            usage
            ;;
        --no-show)
            NO_SHOW_FLAG="--no-show"
            shift
            ;;
        --output-dir)
            OUTPUT_DIR="$2"
            shift 2
            ;;
        -*)
            print_error "Option inconnue: $1"
            usage
            ;;
        *)
            # Premier argument non-option = chemin CSV
            if [ -z "$CSV_PATH" ]; then
                CSV_PATH="$1"
            else
                print_error "Trop d'arguments positionnels"
                usage
            fi
            shift
            ;;
    esac
done

# ============================================================
# CHARGEMENT DES VARIABLES D'ENVIRONNEMENT
# ============================================================
print_header "Analyse Amdahl - MapReduce"

# Charger .env si présent
if [ -f ".env" ]; then
    source .env
    print_success "Variables chargées depuis .env"
fi

# Déterminer le chemin CSV (priorité: argument > .env > défaut)
if [ -z "$CSV_PATH" ]; then
    CSV_PATH="${AMDAL_RESULT_FILE_PATH:-./amdahl_results.csv}"
    print_info "Utilisation du chemin depuis .env: $CSV_PATH"
else
    print_info "Utilisation du chemin fourni: $CSV_PATH"
fi

# Déterminer le répertoire de sortie
if [ -z "$OUTPUT_DIR" ]; then
    OUTPUT_DIR="./amdahl_analysis"
fi

ANALYSIS_SCRIPT="analyze_amdahl.py"

# ============================================================
# VALIDATION
# ============================================================

# Vérifier que le fichier CSV existe
if [ ! -f "$CSV_PATH" ]; then
    print_error "Fichier de résultats introuvable: $CSV_PATH"
    echo ""
    echo "Solutions possibles:"
    echo "  1. Définir AMDAL_RESULT_FILE_PATH dans .env"
    echo "  2. Passer le chemin en argument: $0 /chemin/vers/results.csv"
    echo "  3. Vérifier que les tests Amdahl ont bien été exécutés"
    exit 1
fi

# Vérifier que Python 3 est disponible
if ! command -v python3 &> /dev/null; then
    print_error "Python 3 introuvable"
    echo "Installez-le avec: sudo apt install python3"
    exit 1
fi

# Vérifier que le script d'analyse existe
if [ ! -f "$ANALYSIS_SCRIPT" ]; then
    print_error "Script d'analyse introuvable: $ANALYSIS_SCRIPT"
    echo "Créez-le d'abord avec le code fourni précédemment"
    exit 1
fi

# ============================================================
# VÉRIFICATION DES DÉPENDANCES PYTHON
# ============================================================
print_info "Vérification des dépendances Python..."

missing=0

for module in pandas matplotlib; do
    module_import=${module//-/_}  # Convertir python-dotenv en python_dotenv
    if ! python3 -c "import ${module_import}" 2>/dev/null; then
        print_error "Module manquant: $module"
        ((missing++))
    fi
done

if [ $missing -gt 0 ]; then
    print_error "$missing dépendance(s) manquante(s)"
    echo ""
    echo "Installez-les avec:"
    echo "  pip3 install pandas matplotlib python-dotenv"
    exit 1
fi

print_success "Toutes les dépendances sont présentes"

# ============================================================
# AFFICHAGE DU CONTENU DU CSV
# ============================================================
print_header "Aperçu des résultats"
echo "Fichier: $CSV_PATH"
echo ""

if [ -f "$CSV_PATH" ]; then
    # Afficher les 10 premières lignes
    head -n 11 "$CSV_PATH"
    
    # Compter le nombre de lignes (hors header)
    num_lines=$(($(wc -l < "$CSV_PATH") - 1))
    echo ""
    print_info "$num_lines enregistrement(s) trouvé(s)"
    
    if [ $num_lines -eq 0 ]; then
        print_error "Le fichier CSV est vide (hors header)"
        exit 1
    fi
fi

# ============================================================
# LANCEMENT DE L'ANALYSE
# ============================================================
print_header "Lancement de l'analyse"

echo ""
print_info "Génération des graphiques et du rapport..."
print_info "Répertoire de sortie: $OUTPUT_DIR"
echo ""

# Construire la commande Python
PYTHON_CMD="python3 $ANALYSIS_SCRIPT \"$CSV_PATH\""

# Ajouter le flag --no-show si nécessaire
if [ -n "$NO_SHOW_FLAG" ]; then
    PYTHON_CMD="$PYTHON_CMD $NO_SHOW_FLAG"
fi

# Ajouter le répertoire de sortie custom si spécifié
if [ "$OUTPUT_DIR" != "./amdahl_analysis" ]; then
    PYTHON_CMD="$PYTHON_CMD --output-dir \"$OUTPUT_DIR\""
fi

# Lancer le script Python
eval $PYTHON_CMD

exit_code=$?

# ============================================================
# RÉSULTAT
# ============================================================
echo ""

if [ $exit_code -eq 0 ]; then
    print_success "Analyse terminée avec succès"
    echo ""
    print_info "Les résultats se trouvent dans: $OUTPUT_DIR/"
    echo ""
    echo "Fichiers générés:"
    echo "  - speedup_*.png        (Courbe de speedup)"
    echo "  - efficiency_*.png     (Courbe d'efficacité)"
    echo "  - execution_time_*.png (Temps d'exécution)"
    echo "  - combined_*.png       (Vue combinée)"
    echo "  - report_*.txt         (Rapport détaillé)"
    echo ""
    
    # Afficher les fichiers générés
    if [ -d "$OUTPUT_DIR" ]; then
        latest_files=$(ls -t "$OUTPUT_DIR"/*.png "$OUTPUT_DIR"/*.txt 2>/dev/null | head -5)
        if [ -n "$latest_files" ]; then
            echo "Derniers fichiers générés:"
            echo "$latest_files" | while read file; do
                echo "  - $(basename "$file")"
            done
        fi
    fi
else
    print_error "L'analyse a échoué (code: $exit_code)"
    exit $exit_code
fi