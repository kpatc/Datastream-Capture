"""
Logging configuration pour le pipeline CDC
Logging rotatif avec niveaux séparés pour production
"""
import logging
import os
from logging.handlers import RotatingFileHandler, TimedRotatingFileHandler
import sys


def setup_logging(
    log_dir: str = "logs",
    app_name: str = "cdc_pipeline",
    log_level: str = "INFO"
):
    """
    Configure un système de logging professionnel avec:
    - Rotation des fichiers de logs
    - Séparation par niveau (INFO, ERROR)
    - Format structuré pour parsing
    - Console output coloré
    """
    
    # Créer le répertoire de logs
    os.makedirs(log_dir, exist_ok=True)
    
    # Format de log structuré (facilite le parsing)
    log_format = logging.Formatter(
        '%(asctime)s | %(levelname)-8s | %(name)-20s | %(funcName)-15s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Format JSON pour logs (optionnel, pour outils de monitoring)
    json_format = logging.Formatter(
        '{"timestamp": "%(asctime)s", "level": "%(levelname)s", '
        '"logger": "%(name)s", "function": "%(funcName)s", "message": "%(message)s"}',
        datefmt='%Y-%m-%dT%H:%M:%S'
    )
    
    # Logger racine
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, log_level.upper()))
    
    # Supprimer les handlers existants
    root_logger.handlers.clear()
    
    # 1. Console handler (stdout) - tous les niveaux
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(log_format)
    root_logger.addHandler(console_handler)
    
    # 2. Fichier général - rotation quotidienne (30 jours)
    general_handler = TimedRotatingFileHandler(
        filename=os.path.join(log_dir, f'{app_name}.log'),
        when='midnight',
        interval=1,
        backupCount=30,
        encoding='utf-8'
    )
    general_handler.setLevel(logging.INFO)
    general_handler.setFormatter(log_format)
    root_logger.addHandler(general_handler)
    
    # 3. Fichier erreurs uniquement - rotation par taille (10MB, 5 fichiers)
    error_handler = RotatingFileHandler(
        filename=os.path.join(log_dir, f'{app_name}_error.log'),
        maxBytes=10 * 1024 * 1024,  # 10 MB
        backupCount=5,
        encoding='utf-8'
    )
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(log_format)
    root_logger.addHandler(error_handler)
    
    # 4. Fichier JSON pour parsing automatique (optionnel)
    json_handler = RotatingFileHandler(
        filename=os.path.join(log_dir, f'{app_name}_json.log'),
        maxBytes=50 * 1024 * 1024,  # 50 MB
        backupCount=3,
        encoding='utf-8'
    )
    json_handler.setLevel(logging.INFO)
    json_handler.setFormatter(json_format)
    root_logger.addHandler(json_handler)
    
    # Réduire le niveau de log des bibliothèques tierces (bruyantes)
    logging.getLogger('snowflake.connector').setLevel(logging.WARNING)
    logging.getLogger('confluent_kafka').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    
    # Log initial
    logger = logging.getLogger(__name__)
    logger.info(f"Logging system initialized - Level: {log_level}")
    logger.info(f"Logs directory: {os.path.abspath(log_dir)}")
    
    return root_logger


class ContextLogger:
    """
    Logger avec contexte pour tracer les opérations
    Utile pour debugging en production
    """
    
    def __init__(self, logger, **context):
        self.logger = logger
        self.context = context
    
    def _format_message(self, message):
        """Ajoute le contexte au message"""
        if self.context:
            context_str = ' | '.join(f'{k}={v}' for k, v in self.context.items())
            return f"{message} | Context: {context_str}"
        return message
    
    def debug(self, message, **extra_context):
        ctx = {**self.context, **extra_context}
        self.logger.debug(self._format_message(message), extra=ctx)
    
    def info(self, message, **extra_context):
        ctx = {**self.context, **extra_context}
        self.logger.info(self._format_message(message), extra=ctx)
    
    def warning(self, message, **extra_context):
        ctx = {**self.context, **extra_context}
        self.logger.warning(self._format_message(message), extra=ctx)
    
    def error(self, message, **extra_context):
        ctx = {**self.context, **extra_context}
        self.logger.error(self._format_message(message), extra=ctx, exc_info=True)
    
    def critical(self, message, **extra_context):
        ctx = {**self.context, **extra_context}
        self.logger.critical(self._format_message(message), extra=ctx, exc_info=True)
