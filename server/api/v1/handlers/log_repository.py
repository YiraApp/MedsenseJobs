from server.integrations.mongodb import MongoDBClient

class LogRepository:

    @staticmethod
    async def insert_log(log_data: dict):
        db = await MongoDBClient.get_database()
        collection = db["api_logs"]
        await collection.insert_one(log_data)
