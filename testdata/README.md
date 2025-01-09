🚀 Running the Docker Container

To start the environment, simply run the following command in your terminal:
`docker compose up
`

📋 What Happens When You Run the Container?
1.	Basic Tables Creation
The container will automatically create basic tables using the script:
👉 [provisioned-tables.py](spark/provisioned-tables.py)
	
2.	GDELT Tables Creation
It will also create GDELT event tables using: 
👉 [gdelt-tables.py](spark/gdelt-tables.py)

🔍 Accessing the Tables

Once the container is up and running, you can view all the created tables through the web interface:
🌐 URL: http://localhost:9001/
🔑 Credentials: admin:password