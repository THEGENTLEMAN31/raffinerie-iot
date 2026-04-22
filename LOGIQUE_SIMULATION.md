# Logique de Simulation - Raffinerie IoT

Ce document détaille les principes physiques et stochastiques utilisés pour générer des données réalistes dans le simulateur `simulateur_capteurs.py`.

## 1. Modèle de Processus : Échangeur de Chaleur et Pompe de Transfert
Nous simulons deux équipements critiques :
*   **Pipe-101 (Échangeur) :** Surveille la température de sortie d'un fluide préchauffé.
*   **Pump-303 (Pompe) :** Surveille la santé mécanique via les vibrations.

## 2. Principes de Réalisme Physique

### A. Inertie Thermique (Lissage Exponentiel)
Contrairement à un saut aléatoire, une température industrielle possède une inertie.
*   **Formule :** $V_t = (V_{t-1} \times (1 - \alpha)) + (Cible \times \alpha) + \epsilon$
*   $\alpha$ : Facteur d'inertie (ex: 0.05). Plus il est petit, plus le changement est lent.
*   $\epsilon$ : Bruit blanc (micro-oscillations de précision du capteur).

### B. Corrélation et Dérive
Les vibrations d'une pompe ne sont pas purement aléatoires. Elles oscillent autour d'une valeur de base liée à la charge. Une augmentation de la vibration de base indique une dégradation mécanique.

## 3. Scénarios de Fonctionnement (États Stochastiques)

Le simulateur bascule entre différents états de manière imprévisible (probabiliste) :

| État | Probabilité | Description | Impact sur les Données |
| :--- | :--- | :--- | :--- |
| **Normal** | 90% | Fonctionnement nominal. | Oscillations minimes autour du point de consigne. |
| **Surchauffe / Dérive** | 5% | Encrassement ou problème de régulation. | Montée (ou descente) lente et persistante de la température. |
| **Usure Mécanique** | 5% | Problème de roulement ou cavitation. | Augmentation progressive de l'amplitude des vibrations. |

## 4. Avantages pour l'IA (Machine Learning)
1.  **Temporalité :** Le modèle peut apprendre des séquences temporelles (Time Series).
2.  **Anomalies vs Bruit :** L'IA apprend à distinguer le "bruit normal" du capteur d'une "dérive de panne" réelle.
3.  **Stochastique :** Chaque simulation est unique (points de départ et moments de panne aléatoires), évitant le sur-apprentissage (overfitting).
