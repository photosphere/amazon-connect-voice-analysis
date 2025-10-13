import streamlit as st
import boto3
import time
import uuid
import requests
import pandas as pd
from io import BytesIO

st.title("Amazon Connect Voice Analysis")

s3 = boto3.client('s3')
transcribe = boto3.client('transcribe')
translate = boto3.client('translate')

s3_path = st.text_input("S3 Path (bucket/prefix)", value="amdtest0930/silence").strip()
BUCKET_NAME = s3_path.split('/')[0]
PREFIX = '/'.join(s3_path.split('/')[1:]) if '/' in s3_path else ''

tab1, tab2 = st.tabs(["Transcribe", "Manage Files"])

with tab1:
    language = st.selectbox("Transcribe Language", ["es-US", "en-US"], format_func=lambda x: "English" if x == "en-US" else "Spanish")
    translate_lang = st.selectbox("Translate To", ["None", "en", "es", "fr", "de", "zh", "ja"], format_func=lambda x: {"None": "None", "en": "English", "es": "Spanish", "fr": "French", "de": "German", "zh": "Chinese", "ja": "Japanese"}[x])
    
    if 'results' not in st.session_state:
        st.session_state.results = []
    if 'show_results' not in st.session_state:
        st.session_state.show_results = False
    
    col1, col2, col3 = st.columns([2, 3, 7])
    transcribe_clicked = col1.button("Transcribe")
    
    if st.session_state.results:
        df = pd.DataFrame(st.session_state.results)
        buffer = BytesIO()
        with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name='Transcriptions')
        col2.download_button(
            label="Export to Excel",
            data=buffer.getvalue(),
            file_name="transcription_results.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
    else:
        col2.button("Export to Excel", disabled=True)

    if st.session_state.show_results and st.session_state.results:
        for result in st.session_state.results:
            st.subheader(result['File'])
            audio_url = s3.generate_presigned_url('get_object', Params={'Bucket': BUCKET_NAME, 'Key': result['File']}, ExpiresIn=3600)
            st.audio(audio_url)
            st.text_area("Transcript", result['Transcript'], height=150, key=f"{result['File']}_display_transcript")
            if result['Translation']:
                st.text_area("Translation", result['Translation'], height=150, key=f"{result['File']}_display_translation")
            st.divider()

if tab1 and transcribe_clicked:
    if not BUCKET_NAME:
        st.error("Please enter a valid S3 bucket name")
        st.stop()
    params = {'Bucket': BUCKET_NAME}
    if PREFIX:
        params['Prefix'] = PREFIX
    response = s3.list_objects_v2(**params)
    wav_files = [obj['Key'] for obj in response.get('Contents', []) if obj['Key'].endswith('.wav')]
    
    if not wav_files:
        st.warning("No WAV files found in bucket")
    else:
        st.session_state.results = []
        st.session_state.show_results = False
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
                    
                    translation_text = ""
                    if translate_lang != "None" and transcript_text.strip():
                        source_lang = language.split('-')[0]
                        if source_lang != translate_lang:
                            with st.spinner("Translating..."):
                                translation = translate.translate_text(
                                    Text=transcript_text,
                                    SourceLanguageCode=source_lang,
                                    TargetLanguageCode=translate_lang
                                )
                                translation_text = translation['TranslatedText']
                                st.text_area("Translation", translation_text, height=150, key=f"{s3_key}_translation")
                    
                    st.session_state.results.append({
                        'File': s3_key,
                        'Transcript': transcript_text,
                        'Translation': translation_text
                    })
                else:
                    st.error(f"Failed: {s3_key}")
                
                transcribe.delete_transcription_job(TranscriptionJobName=job_name)
            
            st.divider()
        
        st.session_state.show_results = True
        st.rerun()

with tab2:
    if not BUCKET_NAME:
        st.warning("Please enter a valid S3 bucket name")
        st.stop()
    
    st.subheader("Upload WAV File")
    uploaded_file = st.file_uploader("Choose a WAV file", type=['wav'])
    if uploaded_file and st.button("Upload"):
        s3_key = f"{PREFIX}/{uploaded_file.name}" if PREFIX else uploaded_file.name
        s3.upload_fileobj(uploaded_file, BUCKET_NAME, s3_key)
        st.success(f"Uploaded {s3_key}")
        st.rerun()
    
    st.subheader("WAV Files in Bucket")
    params = {'Bucket': BUCKET_NAME}
    if PREFIX:
        params['Prefix'] = PREFIX
    response = s3.list_objects_v2(**params)
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
