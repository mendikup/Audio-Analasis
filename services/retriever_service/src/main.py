"""main entrypoint (delegates to manager)"""

from services.retriever_service.src.manager import Manager

if __name__ == "__main__":
    manager = Manager()
    manager.run_all()
