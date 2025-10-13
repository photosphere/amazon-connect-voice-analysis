import streamlit as st
import boto3
import time
import uuid
import requests

st.title("Amazon Connect Voice Analysis")

s3 = boto3.client('s3')
transcribe = boto3.client('transcribe')
translate = boto3.client('translate')

BUCKET_NAME = st.text_input("S3 Bucket Name", value="amdtest0930")

tab1, tab2 = st.tabs(["Transcribe", "Manage Files"])

with tab1:
    language = st.selectbox("Transcribe Language", ["es-US", "en-US"], format_func=lambda x: "English" if x == "en-US" else "Spanish")
    translate_lang = st.selectbox("Translate To", ["None", "en", "es", "fr", "de", "zh", "ja"], format_func=lambda x: {"None": "None", "en": "English", "es": "Spanish", "fr": "French", "de": "German", "zh": "Chinese", "ja": "Japanese"}[x])

if tab1 and st.button("Transcribe"):
    response = s3.list_objects_v2(Bucket=BUCKET_NAME)
    wav_files = [obj['Key'] for obj in response.get('Contents', []) if obj['Key'].endswith('.wav')]
    
    if not wav_files:
        st.warning("No WAV files found in bucket")
    else:
        for s3_key in wav_files:
            st.subheader(s3_key)
            
            audio_url = s3.generate_presigned_url('get_object', Params={'Bucket': BUCKET_NAME, 'Key': s3_key}, ExpiresIn=3600)
            st.audio(audio_url)
            
            job_name = f"transcribe-{uuid.uuid4()}"
            
            with st.spinner(f"Transcribing {s3_key}..."):
                transcribe.start_transcription_job(
                    TranscriptionJobName=job_name,
                    Media={'MediaFileUri': f's3://{BUCKET_NAME}/{s3_key}'},
                    MediaFormat='wav',
                    LanguageCode=language
                )
                
                while True:
                    status = transcribe.get_transcription_job(TranscriptionJobName=job_name)
                    if status['TranscriptionJob']['TranscriptionJobStatus'] in ['COMPLETED', 'FAILED']:
                        break
                    time.sleep(2)
                
                if status['TranscriptionJob']['TranscriptionJobStatus'] == 'COMPLETED':
                    transcript_uri = status['TranscriptionJob']['Transcript']['TranscriptFileUri']
                    transcript_data = requests.get(transcript_uri).json()
                    transcript_text = transcript_data['results']['transcripts'][0]['transcript']
                    st.text_area("Transcript", transcript_text, height=150, key=f"{s3_key}_transcript")
                    
                    if translate_lang != "None":
                        source_lang = language.split('-')[0]
                        if source_lang != translate_lang:
                            with st.spinner("Translating..."):
                                translation = translate.translate_text(
                                    Text=transcript_text,
                                    SourceLanguageCode=source_lang,
                                    TargetLanguageCode=translate_lang
                                )
                                st.text_area("Translation", translation['TranslatedText'], height=150, key=f"{s3_key}_translation")
                else:
                    st.error(f"Failed: {s3_key}")
                
                transcribe.delete_transcription_job(TranscriptionJobName=job_name)
            
            st.divider()

with tab2:
    st.subheader("Upload WAV File")
    uploaded_file = st.file_uploader("Choose a WAV file", type=['wav'])
    if uploaded_file and st.button("Upload"):
        s3.upload_fileobj(uploaded_file, BUCKET_NAME, uploaded_file.name)
        st.success(f"Uploaded {uploaded_file.name}")
        st.rerun()
    
    st.subheader("WAV Files in Bucket")
    response = s3.list_objects_v2(Bucket=BUCKET_NAME)
    wav_files = [obj['Key'] for obj in response.get('Contents', []) if obj['Key'].endswith('.wav')]
    
    if wav_files:
        for file in wav_files:
            col1, col2 = st.columns([4, 1])
            col1.text(file)
            if col2.button("Delete", key=f"del_{file}"):
                s3.delete_object(Bucket=BUCKET_NAME, Key=file)
                st.success(f"Deleted {file}")
                st.rerun()
    else:
        st.info("No WAV files in bucket")
