# Azkenengo Python bertsiotik abiatuko gara
FROM python

# Kontainerra hasten denean, bere lan direktorioa honakoa izango da
#WORKDIR /flaskProject

# Uneko direktorioan dauzkagun fitxategiak kopiatuko dira karpeta honetan (fitxategi denak)
#COPY . /flaskProject

#pip erabiliz, “requirements.txt” dauden menpekotasunak instalatuko ditugu, kasu honetan, Flask eta mysql-connector-python
RUN pip install paho-mqtt



# Kontainerra 5000 portutik komunikatuko dela adierazten dugu
#EXPOSE 5000

# Ingurune aldagai bat sortuko dugu
#ENV NAME World

# Kontainerra jaurtitzerakoan, honako komandoa exekutatuko da
CMD ["python", "/tmp/programa_proba.py"]
