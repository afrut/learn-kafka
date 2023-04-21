# Description
A playarea for kafka.

# Setup
- Initialize environment variables.
```
.\env.ps1
```
- Create a Python environment.
```
python -m venv .\python\env
```
- Activate environment.
```
.\python\env\Scripts\activate
```
- Install modules.
```
pip install -r .\python\requirements.txt
```

# Start kafka
- Initialize the environment.
```
.\env.ps1
```
- Start kafka zookeeper and brokers.
```
.\start-kafka.ps1
```
- To start a producer, run the following in another powershell window.
  - Initialize environment variables.
  - Activate environment.
  - Start a simple producer.
```
.\env.ps1
.\python\env\Scripts\activate
python .\python\producerSimple.py --continuous --sleep 10 --flush
```
- To start a consumer, run the following in yet another powershell window.
```
.\env.ps1
.\python\env\Scripts\activate
python .\python\consumerSimple.py
```
- Deactivate environment.
```
deactivate
```