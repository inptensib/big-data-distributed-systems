#!/usr/bin/env python3
"""
Script d'analyse de la loi d'Amdahl pour MapReduce distribué.
Trace les courbes de speedup, efficacité et temps d'exécution.

Usage:
    python3 analyze_amdahl.py /path/to/amdahl_results.csv
    python3 analyze_amdahl.py  # Utilise le chemin depuis .env
"""

import sys
import os
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path
from datetime import datetime
import argparse


# Configuration
DEFAULT_CSV_PATH = os.getenv("AMDAL_RESULT_FILE_PATH", "./amdahl_results.csv")
OUTPUT_DIR = None  # Sera défini dans main()

# Style des graphiques
plt.style.use("seaborn-v0_8-darkgrid")
COLORS = {
    "speedup": "#2E86AB",
    "efficiency": "#A23B72",
    "time": "#F18F01",
    "ideal": "#C73E1D",
    "theoretical": "#6A994E",
}


class AmdahlAnalyzer:
    """Analyseur de résultats basé sur la loi d'Amdahl."""

    def __init__(self, csv_path: str):
        self.csv_path = csv_path
        self.df = None
        self.base_time = None  # T(1) pour calculer le speedup
        self.num_cores = None  # Nombre de cœurs physiques (pour modèle théorique)

    def load_data(self):
        """Charge et nettoie les données CSV."""
        print(f"📂 Chargement de {self.csv_path}...")

        try:
            self.df = pd.read_csv(self.csv_path)
            print(f"   ✓ {len(self.df)} enregistrements chargés")

            # Convertir timestamp en datetime
            self.df["timestamp"] = pd.to_datetime(self.df["timestamp"])

            # Trier par nombre de workers
            self.df = self.df.sort_values("num_workers")

            # Grouper par (num_workers, num_splits) et prendre la médiane
            # pour éliminer les outliers
            self.df = self.df.groupby(
                ["num_workers", "num_splits"], as_index=False
            ).agg(
                {
                    "execution_time_sec": "median",
                    "total_data_size_mb": "first",
                    "timestamp": "last",
                }
            )

            print(f"   ✓ Données agrégées: {len(self.df)} points de mesure")

            # Afficher un résumé
            print("\n📊 Résumé des données:")
            print(
                f"   Workers: {self.df['num_workers'].min()} → {self.df['num_workers'].max()}"
            )
            print(f"   Splits: {self.df['num_splits'].unique()}")
            print(f"   Taille données: {self.df['total_data_size_mb'].iloc[0]:.2f} MB")

        except FileNotFoundError:
            print(f"❌ Erreur: Fichier {self.csv_path} introuvable")
            sys.exit(1)
        except Exception as e:
            print(f"❌ Erreur lors du chargement: {e}")
            sys.exit(1)

    def calculate_metrics(self):
        """Calcule speedup, efficacité et modèle théorique."""
        print("\n🔢 Calcul des métriques...")

        # Temps de référence T(1) : temps avec 1 worker (ou le minimum)
        min_workers = self.df["num_workers"].min()
        self.base_time = self.df[self.df["num_workers"] == min_workers][
            "execution_time_sec"
        ].iloc[0]

        print(f"   Temps de référence T({min_workers}): {self.base_time:.2f}s")

        # Speedup réel : S(n) = T(1) / T(n)
        self.df["speedup_real"] = self.base_time / self.df["execution_time_sec"]

        # Efficacité : E(n) = S(n) / n
        self.df["efficiency"] = self.df["speedup_real"] / self.df["num_workers"]

        # Speedup idéal (linéaire)
        self.df["speedup_ideal"] = self.df["num_workers"]

        # Estimation de la fraction parallèle (p) avec la loi d'Amdahl
        # S(n) = 1 / ((1-p) + p/n)
        # En inversant: p ≈ 1 - (1/S(n) - 1/n) / (1 - 1/n)
        max_workers = self.df["num_workers"].max()
        speedup_max = self.df[self.df["num_workers"] == max_workers][
            "speedup_real"
        ].iloc[0]

        # Formule simplifiée: p = (n * S(n) - n) / (n * S(n) - 1)
        p_estimated = (max_workers * speedup_max - max_workers) / (
            max_workers * speedup_max - 1
        )
        p_estimated = max(0.0, min(1.0, p_estimated))  # Clamp entre 0 et 1

        print(
            f"   Fraction parallèle estimée: {p_estimated:.3f} ({p_estimated * 100:.1f}%)"
        )
        print(
            f"   Fraction séquentielle: {1 - p_estimated:.3f} ({(1 - p_estimated) * 100:.1f}%)"
        )

        # Speedup théorique selon Amdahl: S(n) = 1 / ((1-p) + p/n)
        self.df["speedup_theoretical"] = 1 / (
            (1 - p_estimated) + p_estimated / self.df["num_workers"]
        )

        # Speedup max théorique: S_max = 1 / (1-p)
        speedup_max_theoretical = 1 / (1 - p_estimated)
        print(f"   Speedup maximum théorique: {speedup_max_theoretical:.2f}x")

        print("   ✓ Métriques calculées")

    def plot_speedup(self):
        """Trace la courbe de speedup (réel vs idéal vs théorique)."""
        print("\n📈 Génération du graphique de speedup...")

        fig, ax = plt.subplots(figsize=(12, 7))

        workers = self.df["num_workers"]

        # Speedup réel (mesures)
        ax.plot(
            workers,
            self.df["speedup_real"],
            marker="o",
            linewidth=2.5,
            markersize=8,
            color=COLORS["speedup"],
            label="Speedup réel (mesuré)",
        )

        # Speedup idéal (linéaire)
        ax.plot(
            workers,
            self.df["speedup_ideal"],
            linestyle="--",
            linewidth=2,
            alpha=0.7,
            color=COLORS["ideal"],
            label="Speedup idéal (linéaire)",
        )

        # Speedup théorique (loi d'Amdahl)
        ax.plot(
            workers,
            self.df["speedup_theoretical"],
            linestyle=":",
            linewidth=2,
            alpha=0.8,
            color=COLORS["theoretical"],
            label="Speedup théorique (Amdahl)",
        )

        # Annotations
        max_speedup = self.df["speedup_real"].max()
        max_workers = self.df.loc[self.df["speedup_real"].idxmax(), "num_workers"]
        ax.annotate(
            f"Max: {max_speedup:.2f}x\n({max_workers} workers)",
            xy=(max_workers, max_speedup),
            xytext=(10, 10),
            textcoords="offset points",
            fontsize=10,
            fontweight="bold",
            bbox=dict(boxstyle="round,pad=0.5", facecolor="yellow", alpha=0.7),
            arrowprops=dict(arrowstyle="->", connectionstyle="arc3,rad=0"),
        )

        ax.set_xlabel("Nombre de workers", fontsize=13, fontweight="bold")
        ax.set_ylabel("Speedup (×)", fontsize=13, fontweight="bold")
        ax.set_title(
            "Loi d'Amdahl : Speedup vs Nombre de Workers",
            fontsize=15,
            fontweight="bold",
            pad=20,
        )
        ax.legend(fontsize=11, loc="upper left")
        ax.grid(True, alpha=0.3)
        ax.set_xlim(left=0)
        ax.set_ylim(bottom=0)

        # Sauvegarder
        output_path = (
            OUTPUT_DIR / f"speedup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png"
        )
        plt.tight_layout()
        plt.savefig(output_path, dpi=300, bbox_inches="tight")
        print(f"   ✓ Graphique sauvegardé: {output_path}")

        return fig

    def plot_efficiency(self):
        """Trace la courbe d'efficacité."""
        print("\n📉 Génération du graphique d'efficacité...")

        fig, ax = plt.subplots(figsize=(12, 7))

        workers = self.df["num_workers"]
        efficiency_pct = self.df["efficiency"] * 100

        ax.plot(
            workers,
            efficiency_pct,
            marker="s",
            linewidth=2.5,
            markersize=8,
            color=COLORS["efficiency"],
            label="Efficacité réelle",
        )

        # Ligne de référence 100%
        ax.axhline(
            y=100,
            linestyle="--",
            linewidth=2,
            alpha=0.5,
            color="gray",
            label="Efficacité idéale (100%)",
        )

        # Zone de bonne efficacité (>80%)
        ax.axhspan(80, 100, alpha=0.1, color="green", label="Zone efficace (>80%)")

        # Annotations
        min_efficiency = efficiency_pct.min()
        min_workers = self.df.loc[efficiency_pct.idxmin(), "num_workers"]
        ax.annotate(
            f"Min: {min_efficiency:.1f}%\n({min_workers} workers)",
            xy=(min_workers, min_efficiency),
            xytext=(10, -30),
            textcoords="offset points",
            fontsize=10,
            fontweight="bold",
            bbox=dict(boxstyle="round,pad=0.5", facecolor="orange", alpha=0.7),
            arrowprops=dict(arrowstyle="->", connectionstyle="arc3,rad=0"),
        )

        ax.set_xlabel("Nombre de workers", fontsize=13, fontweight="bold")
        ax.set_ylabel("Efficacité (%)", fontsize=13, fontweight="bold")
        ax.set_title(
            "Efficacité du Parallélisme", fontsize=15, fontweight="bold", pad=20
        )
        ax.legend(fontsize=11, loc="upper right")
        ax.grid(True, alpha=0.3)
        ax.set_xlim(left=0)
        ax.set_ylim(0, 110)

        # Sauvegarder
        output_path = (
            OUTPUT_DIR / f"efficiency_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png"
        )
        plt.tight_layout()
        plt.savefig(output_path, dpi=300, bbox_inches="tight")
        print(f"   ✓ Graphique sauvegardé: {output_path}")

        return fig

    def plot_execution_time(self):
        """Trace le temps d'exécution vs nombre de workers."""
        print("\n⏱️  Génération du graphique de temps d'exécution...")

        fig, ax = plt.subplots(figsize=(12, 7))

        workers = self.df["num_workers"]
        time_sec = self.df["execution_time_sec"]

        ax.plot(
            workers,
            time_sec,
            marker="D",
            linewidth=2.5,
            markersize=8,
            color=COLORS["time"],
            label="Temps d'exécution mesuré",
        )

        # Temps idéal (T(n) = T(1) / n)
        time_ideal = self.base_time / workers
        ax.plot(
            workers,
            time_ideal,
            linestyle="--",
            linewidth=2,
            alpha=0.7,
            color=COLORS["ideal"],
            label="Temps idéal (linéaire)",
        )

        # Annotations
        min_time = time_sec.min()
        min_time_workers = self.df.loc[time_sec.idxmin(), "num_workers"]
        ax.annotate(
            f"Min: {min_time:.1f}s\n({min_time_workers} workers)",
            xy=(min_time_workers, min_time),
            xytext=(10, 20),
            textcoords="offset points",
            fontsize=10,
            fontweight="bold",
            bbox=dict(boxstyle="round,pad=0.5", facecolor="lightgreen", alpha=0.7),
            arrowprops=dict(arrowstyle="->", connectionstyle="arc3,rad=0"),
        )

        ax.set_xlabel("Nombre de workers", fontsize=13, fontweight="bold")
        ax.set_ylabel("Temps d'exécution (secondes)", fontsize=13, fontweight="bold")
        ax.set_title(
            "Temps d'Exécution vs Nombre de Workers",
            fontsize=15,
            fontweight="bold",
            pad=20,
        )
        ax.legend(fontsize=11, loc="upper right")
        ax.grid(True, alpha=0.3)
        ax.set_xlim(left=0)
        ax.set_ylim(bottom=0)

        # Sauvegarder
        output_path = (
            OUTPUT_DIR
            / f"execution_time_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png"
        )
        plt.tight_layout()
        plt.savefig(output_path, dpi=300, bbox_inches="tight")
        print(f"   ✓ Graphique sauvegardé: {output_path}")

        return fig

    def plot_combined(self):
        """Graphique combiné (speedup + efficacité)."""
        print("\n📊 Génération du graphique combiné...")

        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 7))

        workers = self.df["num_workers"]

        # Speedup (gauche)
        ax1.plot(
            workers,
            self.df["speedup_real"],
            marker="o",
            linewidth=2.5,
            markersize=8,
            color=COLORS["speedup"],
            label="Speedup réel",
        )
        ax1.plot(
            workers,
            self.df["speedup_ideal"],
            linestyle="--",
            linewidth=2,
            alpha=0.7,
            color=COLORS["ideal"],
            label="Speedup idéal",
        )
        ax1.plot(
            workers,
            self.df["speedup_theoretical"],
            linestyle=":",
            linewidth=2,
            alpha=0.8,
            color=COLORS["theoretical"],
            label="Modèle Amdahl",
        )
        ax1.set_xlabel("Nombre de workers", fontsize=12, fontweight="bold")
        ax1.set_ylabel("Speedup (×)", fontsize=12, fontweight="bold")
        ax1.set_title("Speedup", fontsize=13, fontweight="bold")
        ax1.legend(fontsize=10)
        ax1.grid(True, alpha=0.3)
        ax1.set_xlim(left=0)
        ax1.set_ylim(bottom=0)

        # Efficacité (droite)
        efficiency_pct = self.df["efficiency"] * 100
        ax2.plot(
            workers,
            efficiency_pct,
            marker="s",
            linewidth=2.5,
            markersize=8,
            color=COLORS["efficiency"],
            label="Efficacité",
        )
        ax2.axhline(
            y=100,
            linestyle="--",
            linewidth=2,
            alpha=0.5,
            color="gray",
            label="Idéal (100%)",
        )
        ax2.axhspan(80, 100, alpha=0.1, color="green")
        ax2.set_xlabel("Nombre de workers", fontsize=12, fontweight="bold")
        ax2.set_ylabel("Efficacité (%)", fontsize=12, fontweight="bold")
        ax2.set_title("Efficacité", fontsize=13, fontweight="bold")
        ax2.legend(fontsize=10)
        ax2.grid(True, alpha=0.3)
        ax2.set_xlim(left=0)
        ax2.set_ylim(0, 110)

        fig.suptitle(
            "Analyse de Performance - Loi d'Amdahl",
            fontsize=16,
            fontweight="bold",
            y=1.02,
        )

        # Sauvegarder
        output_path = (
            OUTPUT_DIR / f"combined_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png"
        )
        plt.tight_layout()
        plt.savefig(output_path, dpi=300, bbox_inches="tight")
        print(f"   ✓ Graphique sauvegardé: {output_path}")

        return fig

    def generate_report(self):
        """Génère un rapport texte."""
        print("\n📝 Génération du rapport...")

        report_path = (
            OUTPUT_DIR / f"report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        )

        with open(report_path, "w", encoding="utf-8") as f:
            f.write("=" * 60 + "\n")
            f.write("   RAPPORT D'ANALYSE AMDAHL - MAPREDUCE DISTRIBUÉ\n")
            f.write("=" * 60 + "\n\n")

            f.write(f"📅 Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"📂 Source: {self.csv_path}\n")
            f.write(f"📊 Points de mesure: {len(self.df)}\n\n")

            f.write("--- CONFIGURATION ---\n")
            f.write(
                f"Taille du dataset: {self.df['total_data_size_mb'].iloc[0]:.2f} MB\n"
            )
            f.write(f"Nombre de splits: {self.df['num_splits'].iloc[0]}\n")
            f.write(
                f"Workers testés: {self.df['num_workers'].min()} → {self.df['num_workers'].max()}\n\n"
            )

            f.write("--- RÉSULTATS ---\n")
            f.write(f"Temps de référence T(1): {self.base_time:.2f}s\n")
            f.write(
                f"Temps minimum: {self.df['execution_time_sec'].min():.2f}s "
                f"({self.df.loc[self.df['execution_time_sec'].idxmin(), 'num_workers']} workers)\n"
            )
            f.write(
                f"Speedup maximum: {self.df['speedup_real'].max():.2f}x "
                f"({self.df.loc[self.df['speedup_real'].idxmax(), 'num_workers']} workers)\n"
            )
            f.write(f"Efficacité maximale: {self.df['efficiency'].max() * 100:.1f}%\n")
            f.write(
                f"Efficacité minimale: {self.df['efficiency'].min() * 100:.1f}%\n\n"
            )

            f.write("--- DÉTAILS PAR CONFIGURATION ---\n")
            f.write(
                self.df[
                    ["num_workers", "execution_time_sec", "speedup_real", "efficiency"]
                ].to_string(index=False)
            )
            f.write("\n\n")

            f.write("--- INTERPRÉTATION ---\n")
            avg_efficiency = self.df["efficiency"].mean() * 100
            if avg_efficiency > 80:
                f.write("✅ Excellente scalabilité (efficacité moyenne > 80%)\n")
            elif avg_efficiency > 60:
                f.write("⚠️  Scalabilité acceptable (efficacité moyenne 60-80%)\n")
            else:
                f.write("❌ Scalabilité limitée (efficacité moyenne < 60%)\n")
                f.write(
                    "   Recommandation: Réduire la fraction séquentielle ou le overhead de communication\n"
                )

        print(f"   ✓ Rapport sauvegardé: {report_path}")

    def run_analysis(self):
        """Lance l'analyse complète."""
        print("\n" + "=" * 60)
        print("   ANALYSE AMDAHL - MAPREDUCE DISTRIBUÉ")
        print("=" * 60)

        self.load_data()
        self.calculate_metrics()

        # Générer tous les graphiques
        self.plot_speedup()
        self.plot_efficiency()
        self.plot_execution_time()
        self.plot_combined()

        # Générer le rapport
        self.generate_report()

        print("\n" + "=" * 60)
        print(f"✅ Analyse terminée ! Résultats dans: {OUTPUT_DIR}/")
        print("=" * 60)

        # Afficher les graphiques
        plt.show()


def main():
    """Point d'entrée du script."""
    global OUTPUT_DIR
    
    # Parser les arguments
    parser = argparse.ArgumentParser(description='Analyse des résultats Amdahl')
    parser.add_argument('csv_path', nargs='?', default=None,
                       help='Chemin vers le fichier CSV (défaut: AMDAL_RESULT_FILE_PATH depuis .env)')
    parser.add_argument('--output-dir', type=str, default='./amdahl_analysis',
                       help='Répertoire de sortie (défaut: ./amdahl_analysis)')
    parser.add_argument('--no-show', action='store_true',
                       help='Ne pas afficher les graphiques')
    args = parser.parse_args()
    
    # Définir le répertoire de sortie
    OUTPUT_DIR = Path(args.output_dir)
    OUTPUT_DIR.mkdir(exist_ok=True)
    
    # Déterminer le chemin du CSV
    if args.csv_path:
        csv_path = args.csv_path
    else:
        csv_path = os.getenv("AMDAL_RESULT_FILE_PATH", DEFAULT_CSV_PATH)
    
    # Lancer l'analyse
    analyzer = AmdahlAnalyzer(csv_path)
    analyzer.run_analysis()
    
    # Afficher les graphiques seulement si --no-show n'est pas spécifié
    if not args.no_show:
        plt.show()


if __name__ == "__main__":
    main()
