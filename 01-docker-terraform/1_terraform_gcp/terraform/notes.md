# How to create a SSH key for GCP
1. Run the following command:
     
     ssh-keygen -t rsa -f ~/.ssh/FILE_NAME -C USERNAME

2. Copy public key in Metada > SSH KEYS


# Create an instance
1. VM Instance > Create Instances

2. Give it a name

3. Select region

4. See monthly estimate

5. Do not get a huge instance

6. Choose Machine type (one comparable to your laptop, and see cost)

7. Choose operating system. (Ubuntu) and size (30GB)

8. Check gcloud CLI (command-line)

9. Once create copy external IP

10. Run the following command
    ssh -i ~/.ssh/gcp_key rtrlpz@IP_ADDRESS


# After inside of the VM Instance
1. Run the following    
    commands htop ls gcloud --version

2. Install Anaconda - Linux version (wget)

3. Run
     bash  ANACONDA.sh file


# Create a config file
1. Run 
    touche CONFIG_NAME

2. code config (Open with cost)

3. add 
Host INSTANCE_NAME
    HostName EXTERNAL_IP
    User rtrlpz
    IdentityFile ~/.ssh/gcp_key

4. Run 
    shh INSTANCE_NAME 
    
# Download Remote - SSH
1. Go to extensions and download REMOTE SSH 

2. Click on >< at the left bottom of the screen

3. Connect to host and select the GCP host

# Install docker
1. Run
    sudo apt-get update
    sudo apt-get install docker.it

# Clone course repository
1. Select HTTPS link and run:
    git clone LINK

# Prevent having to use sudo every time we run docker
1. run:
    sudo groupadd docker
    sudo gpasswd -a $USER docker

2. restart, run:
    sudo service docker restart
    
3. Log out and log back in
    ssh INSTANCE NAME

# Install docker compose
1. Go to github docker compose

2. Select the latest version (linux x86 64)

3. Create folder "bin' and move to it

4. run:
    wget GITHUB LINK -O docker-compose

5. Make it executale
    chmod +x docker-compose

6. run:
    ./docker-compose version

# Shortcut for using docker-compse without ./
1. run:
    nano .bashrc
2. add at the:
    export PATH="${HOME}/bin:${PATH}"

# Run week 1
1. Go to week one folder

2. run:
    docker-compose up -d

3. verify:
    docker ps

# Install pcgli
1. run:
    pip install pgcli

    pgcli -h localhost -U ROOT -d ny_taxi 

    pip uninstall pgcli

# Open working directory with visual code
1. Go to README.md and seach for conda pgcli install

2. run:
    conda install -c conda-forge pgcli

    pip install -U mycli

    pgcli -h localhost -U root -d ny_taxi

3. \dt

# How to connect postgres to the local machine
1. Go to port section in virtual code

2. Add postgres 5432, pgadmin 8080, jupyter notebook 8888 

3. Run: 
        pgcli -h localhost -U root -d ny_taxi

3. \dt
 

# Open jupyter notebook
1. Run wget tripdata download link

2. Get to jupyer notebook and un the upload-data notebook

# Download terraform
1. Download binary

2. unzip file name (pip install unzip)

3. rm zip file

4. terraform -version

5. go to main.tf variables.tf


Here’s a structured, reproducible checklist for setting up and tearing down a GCP Cloud VM environment tailored to your workflow hygiene and remote legal/data ops needs:

⚙️ GCP Cloud VM Setup Workflow
🔐 1. Generate SSH Keys

☁️ 2. Create VM on GCP
• 	Go to Google Cloud Console
• 	Navigate to Compute Engine > VM Instances
• 	Create instance with:
• 	OS: Ubuntu 22.04 LTS
• 	Firewall: allow HTTP/HTTPS if needed
• 	Add your public SSH key under “Security”
🔗 3. Connect via SSH

🐍 4. Install Anaconda

🐳 5. Install Docker

🗂️ 6. Create SSH Config File
Edit :

💻 7. VS Code Remote Access
• 	Install Remote - SSH extension
• 	Connect using  from SSH config
🧩 8. Install docker-compose

🐘 9. Install pgcli

🔁 10. Port Forwarding (VS Code)
In  or via SSH config:

• 	Access Jupyter: 
• 	Access pgAdmin:  (if running via Docker)
🌍 11. Install Terraform

📁 12. Transfer Credentials via SFTP

🧹 13. Shutdown & Remove VM
