# VideoLoggingAndRecovery
CS8803-DSI project that explores optimizing logging and recovery of video data in a DBMS.

## Setup

1. Create a virtualenv: `python3 -m venv venv`
2. Activate the virtualenv: `source venv/bin/activate`
3. Upgrade pip: `pip install --upgrade pip`
4. Install dependencies: `pip install -r requirements.txt`
5. Install Java, this can be done on Ubuntu with `sudo apt install openjdk-8-jdk openjdk-8-jre`.


## Datasets

The VisualRoad dataset used can be downloaded from <https://drive.google.com/uc?export=download&id=1waiXfYGPo_kJcuPDCcKg4aXgyGY1bZkR>. Place the files, e.g. traffic-001.mp4, into the data folder.
For compatibility with Petastorm, any file you want to use must have all dashes removed. E.g. "traffic-001.mp4" should be renamed to "traffic001.mp4".
You can run the script `python generate_video_prefixes.py` to create shortened version of the traffic001.mp4 video file for testing purposes.

## Testing

The unit tests can be run with the command `python -m unittest`.