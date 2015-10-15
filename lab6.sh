git clone https://github.com/UC-Berkeley-I-School/w205-labs-exercises.git
cd w205-labs-exercises/data/Crimes_-_2001_to_present_data/
cat x* > Crimes_-_2001_to_present.csv.gz
gunzip Crimes_-_2001_to_present.csv.gz
mv Crimes_-_2001_to_present.csv /root
cp w205-labs-exercises/data/weblog.csv /root