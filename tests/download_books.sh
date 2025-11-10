#!/bin/bash

# Script pour télécharger des livres de test depuis Project Gutenberg

# 1. Définir le dossier de destination
TARGET_DIR="books"

# 2. S'assurer que le dossier existe
mkdir -p $TARGET_DIR

# 3. Définir les livres à télécharger (URL et Nom de Fichier)
# Format: "URL_SOURCE  NOM_DU_FICHIER.txt"
BOOKS=(
    "https://www.gutenberg.org/ebooks/84.txt.utf-8 frankenstein.txt"
    "https://www.gutenberg.org/ebooks/345.txt.utf-8 dracula.txt"
    "https://www.gutenberg.org/ebooks/1184.txt.utf-8 monte_cristo_fr.txt"
    "https://www.gutenberg.org/ebooks/135.txt.utf-8 les_miserables_fr.txt"
    "https://www.gutenberg.org/ebooks/11.txt.utf-8 alice_in_wonderland.txt"
)

echo "Démarrage du téléchargement de ${#BOOKS[@]} livres dans le dossier '$TARGET_DIR'..."

# 4. Boucler sur la liste et télécharger chaque livre
for book_info in "${BOOKS[@]}"; do
    # Divise la chaîne "URL Fichier" en deux variables
    read -r url filename <<< "$book_info"
    
    echo " -> Téléchargement de $filename..."
    
    # Utilise wget:
    # -q : Mode "quiet" (silencieux), pour ne pas polluer le log
    # -O : Spécifie le fichier de sortie (Output)
    wget -q -O "$TARGET_DIR/$filename" "$url"
    
    # Vérifie si la commande précédente (wget) a réussi
    if [ $? -eq 0 ]; then
        echo "    ... Terminé."
    else
        echo "    ... ERREUR lors du téléchargement de $filename."
    fi
done

echo "Téléchargement complet. Les fichiers sont dans '$TARGET_DIR/'."