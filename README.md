Projet ETL - Données footballistiques
====================================

Ce dépôt contient un pipeline ETL complet qui consolide les matchs des Coupes du Monde de la FIFA de 1930 à 2022, applique des règles de nettoyage homogènes puis charge le tout dans une base PostgreSQL prête pour la data analyse ou la BI.

Ce projet a été réalisé dans le cadre du deuxième brief de la formation Data Engineer par Ahmed SKANJI, Alexandre SEVERIEN, Ashley DE HEIDEN et Kaouter RHAZLANI.

Structure du projet
-------------------

- `main.py` : point d'entrée. Orchestre extraction (`src/etl/extract.py`), transformation (`src/etl/transform.py`) et chargement (`src/etl/load.py`).
- `config.yaml` : paramètre les chemins vers les sources ainsi que toutes les règles de mapping/normalisation (colonnes à conserver, harmonisation des phases, renommages, etc.).
- `src/etl/extract.py` : lecture robuste de CSV et JSON imbriqués, y compris le parsing spécifique du JSON 2018.
- `src/etl/transform.py` : fonctions dédiées à chaque millésime (2010, 2014, 2018, 2022) pour normaliser les colonnes, nettoyer les valeurs et fabriquer des identifiants de match cohérents.
- `src/etl/load.py` & `src/etl/utils.py` : création du moteur SQLAlchemy, exécution des requêtes et helpers génériques (chargement de config, nettoyage des chaînes, conversions de dates...).
- `data/` : sources brutes.
  - `matches_19302010.csv` – historique 1930-2010.
  - `WorldCupMatches2014.csv` – Coupe du Monde 2014.
  - `data_2018.json` – Coupe du Monde 2018 (structure imbriquée).
  - `Fifa_world_cup_matches.csv` – Coupe du Monde 2022 (dataset Kaggle : <https://www.kaggle.com/datasets/die9origephit/fifa-world-cup-2022-complete-dataset>).
- `notebook/` et `src/eda/` : notebooks d’extraction/EDA pour comprendre les jeux de données et tester les transformations avant industrialisation.
- `.env` : variables de connexion PostgreSQL (voir exemple ci-dessous).

Prérequis
---------

- Python 3.10+
- Accès à une base PostgreSQL (locale ou distante)

Installation rapide
-------------------

```bash
python -m venv .venv
source .venv/bin/activate  # Windows : .\.venv\Scripts\activate
pip install pandas numpy SQLAlchemy psycopg2-binary python-dotenv PyYAML pyparsing
```

Configuration
-------------

1. **Chemins des fichiers** : ajuster `config.yaml` si vos sources ne sont pas dans `data/`.
2. **Règles de transformation** : `config.yaml` définit les mappings de colonnes, la normalisation des phases (`stage`), les colonnes conservées, etc. Toute nouvelle règle doit être ajoutée ici pour rester centralisée.
3. **Connexion PostgreSQL** : créer un fichier `.env` à la racine avec :

   ```
   HOST="votre_hote"
   DATABASE="votre_base"
   USER="votre_user"
   PASSWORD="votre_mot_de_passe"
   ```

   Ces variables sont utilisées dans `main.py` pour initialiser le moteur SQLAlchemy.

Exécution du pipeline
---------------------

```bash
python main.py
```

Le script :

1. Lit les fichiers configurés.
2. Applique les transformations spécifiques à chaque édition.
3. Concatène tous les matchs, recrée un `match_id` unique et trie par date.
4. Crée la table `matches` (schéma : `match_id`, `date`, `home_team`, `away_team`, `home_result`, `away_result`, `stage`, `edition`, `city`) et charge les données dans PostgreSQL via `to_sql`.

Ressources supplémentaires
--------------------------

- Les notebooks du dossier `notebook/` détaillent l'extraction et les validations par édition.
- Les notebooks `src/eda/*.ipynb` montrent l’analyse exploratoire réalisée par les différents membres de l’équipe.

N’hésitez pas à adapter la configuration, ajouter de nouvelles éditions aux fonctions de `src/etl/transform.py` ou brancher d’autres cibles de chargement (ex : data warehouse) en vous appuyant sur le module `load`.
