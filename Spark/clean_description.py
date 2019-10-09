import re
from termcolor import colored
from nltk.stem import WordNetLemmatizer
from pyspark.ml.feature import StopWordsRemover
from pyspark.ml.feature import Tokenizer
from pyspark.sql.functions import udf, explode, count
from pyspark.sql.types import ArrayType, StringType

'''
Converts messy body of texts into word stems
'''

def remove_punctuation(body):
    """
    - Removes punctuation, symbols, and numbers
    - Changes to lower case
    - Strips new lines and extra spaces
    Args:
        body: a body of text
    Returns:
        A string with clean-up procedures applied
    """
    punctuation_removed = re.sub(r'[^\sa-zA-Z]', ' ', str(body))
    spaces_removed = re.sub(r'\s+', ' ', punctuation_removed, flags=re.DOTALL)
    line_removed = spaces_removed.replace("\n", " ")
    return str(line_removed)

def lemmatize(tokens):
    """
    Generate word lemma, help to reduce total words.
    Only applies to variations of nouns
    Args:
        tokens: a list of words
    Returns:
        stems: a list of word lemma
    """
    wordnet_lemmatizer = WordNetLemmatizer()
    stems = [wordnet_lemmatizer.lemmatize(token) for token in tokens if len(token) > 1]
    return stems

def clean_description(jd):
    """
    Apply the procedures to clean up the job description field
    Args:
        jd: the dataframe generated from Parquet files
    Returns:
        jd_stemmed: A dataframe with clean-up procedures applied to the
            job_description field
    """
    print(colored("[Starting]: Cleaning Job Description", "green"))
    print(colored("[PROCESSING]: Removing Punctuations", "red"))
    punc_remover = udf(lambda body: remove_punctuation(body), StringType())
    jd_p_removed = jd.withColumn("p_removed", punc_remover("job_description"))

    print(colored("[PROCESSING]: Tokenizing Text Vectors", "red"))
    tokenizer = Tokenizer(inputCol="p_removed", outputCol="tokenized")
    jd_tokenized = tokenizer.transform(jd_p_removed)

    print(colored("[PROCESSING]: Removing Stopwords", "red"))
    sw_remover = StopWordsRemover(inputCol="tokenized", outputCol="sw_removed")
    jd_sw_removed = sw_remover.transform(jd_tokenized)

    print(colored("[PROCESSING]: Stemming Tokenized Text", "red"))
    stem = udf(lambda tokens: lemmatize(tokens), ArrayType(StringType()))
    jd_stemmed = jd_sw_removed.withColumn("stemmed", stem("sw_removed"))

    print(colored("[Finished] Cleaning", "green"))
    jd_cleaned = jd_stemmed.select('uniq_id', 'job_title', 'company_name',
        'state', 'post_date', 'job_description', 'stemmed')
    return jd_cleaned
