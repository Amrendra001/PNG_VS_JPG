import os

# vipshome = 'C:/vips-dev-8.14/bin/'
# os.environ['PATH'] = vipshome + ';' + os.environ['PATH']

import pandas as pd
import numpy as np
import boto3
from tqdm import tqdm
from botocore.errorfactory import ClientError
import time
from multiprocessing import Process, Pipe, cpu_count
import shutil
import random
import pyvips
import fitz
from pdf2image import convert_from_path
import pypdfium2 as pdfium

TEMP_FOLDER = 'pdf_path/'
TEMP_IMAGE_DIR = 'tmp_image_dir/'
if not os.path.isdir(TEMP_IMAGE_DIR):
    os.mkdir(TEMP_IMAGE_DIR)

PHARMA_PO_1INDEX = [
    'PHARMA_TEST_2022522335250824',
    'PHARMA_TEST_20225233112878569',
    'PHARMA_TEST_202252355359932158',
    'PHARMA_TEST_202251652214336372',
    'PHARMA_TEST_20225200395584273',
    'PHARMA_TEST_20225223416991825',
    'PHARMA_TEST_202251695415696775',
    'PHARMA_TEST_20225172131036316',
    'PHARMA_TEST_20225233956529559',
    'PHARMA_TEST_202252112516924385',
    'PHARMA_TEST_2022518111332411122',
    'PHARMA_TEST_2022517101841374355',
    'PHARMA_TEST_202251621628661797',
    'PHARMA_TEST_202251810234262862',
    'PHARMA_TEST_20225223750787832',
    'PHARMA_TEST_202252003927968268',
    'PHARMA_TEST_20225232221406299',
    'PHARMA_TEST_2022518102442312871',
    'PHARMA_TEST_2022518102448274872',
    'PHARMA_TEST_20225223724758831',
    'PHARMA_TEST_202252005527461474',
    'PHARMA_TEST_20225170297196881',
    'PHARMA_TEST_202251695343197768',
    'PHARMA_TEST_20225233858748556',
    'PHARMA_TEST_202252010212547176',
    'PHARMA_TEST_202251721329152319',
    'PHARMA_TEST_20225223541531826',
    'PHARMA_TEST_202252005516138473',
    'PHARMA_TEST_202252403230408828',
    'PHARMA_TEST_202252433322603510',
    'PHARMA_TEST_202251794650540277',
    'PHARMA_TEST_202251695319787763',
    'PHARMA_TEST_202252105550368151',
    'PHARMA_TEST_2022523396741557',
    'PHARMA_TEST_20225165214811365',
    'PHARMA_TEST_20225223327557823',
    'PHARMA_TEST_202251794659128278',
    'PHARMA_TEST_2022520102047872175',
    'PHARMA_TEST_202252112531243387',
    'PHARMA_TEST_2022518102427375868',
]


def child_process_wrapper(runner_func, child_conn, *args):
    output = runner_func(*args)
    child_conn.send([output])
    child_conn.close()


def multiprocessing_handler(runner_func=None, args_list=None):
    outputs_list = []

    if not runner_func or not args_list:
        return outputs_list

    print('Num processes required:', len(args_list))
    print('Num processors available:', cpu_count())

    # Create a list to keep all processes
    processes = []
    # Create a list to keep connections
    connections = []

    # Create a process per argument list
    for arg_list in args_list:
        # Create a pipe for communication
        parent_conn, child_conn = Pipe(duplex=False)
        connections.append((parent_conn, child_conn))
        # Create the process, pass args and connection object
        process = Process(target=child_process_wrapper, args=(runner_func, child_conn, *arg_list))
        processes.append(process)

    # Start all processes
    for process in processes:
        process.start()

    # Receive outputs from all child processes
    for parent_conn, child_conn in connections:
        child_conn.close()  # Ensure child_conn is not open in parent process so parent_conn.recv() doesn't block
        outputs_list.append(parent_conn.recv()[0])

    # Make sure that all processes have finished
    for process in processes:
        process.join()

    return outputs_list


def s3_sync(source, destination):
    sync_command = f"aws s3 sync {source} {destination}"
    os.system(sync_command)


def download_from_s3(key, bucket_name, s3_region):
    if not os.path.isdir(TEMP_FOLDER):
        os.mkdir(TEMP_FOLDER)
        print('Created Temp Folder')
    local_file_name = key.split('/')[-1][:-4]  # key.replace('/', '_')
    pdf_path = f'{TEMP_FOLDER}{local_file_name}.pdf'
    if os.path.exists(pdf_path):
        return pdf_path, True
    s3 = boto3.client('s3', region_name=s3_region)
    try:
        s3.download_file(bucket_name, key, pdf_path)
    except ClientError as e:
        if e.response['Error']['Code'] == "404":
            return key, False
        else:
            raise e
    return pdf_path, True


def create_dict_png_jpg(df, column_name):
    cnt_1 = 0
    cnt_2 = 0
    doc_id_to_png_or_jpg = dict()
    for doc_id, value in zip(df[column_name].value_counts().index, df[column_name].value_counts()):
        if cnt_1 <= cnt_2:
            doc_id_to_png_or_jpg[doc_id] = 'png'
            cnt_1 += value
        else:
            doc_id_to_png_or_jpg[doc_id] = 'jpg'
            cnt_2 += value
    return doc_id_to_png_or_jpg


def create_dict_coversion_method(df, column_name):
    methods = ['pdf2image', 'pyvips', 'pypdfium', 'pymupdf', 'mutool']
    doc_id_to_methods = dict()
    random.seed(47)
    for doc_id, value in zip(df[column_name].value_counts().index, df[column_name].value_counts()):
        doc_id_to_methods[doc_id] = ','.join(random.sample(methods, 3))
    return doc_id_to_methods


def filename_to_doc_id(x):
    if x.startswith('GRN-2327') or x.startswith('GRN_5000689552'):
        return '_'.join(x.split('_')[:2])
    if x.startswith(('JNJ', 'PHARMA', 'DARK', 'Mars', 'MT', 'Panda', 'TAMIMI', 'PO-', 'ALKEM')):
        return '_'.join(x.split('_')[:-1])
    if x.startswith('PO_'):
        return '_'.join(x.split('_')[:-2])
    return x.split('_')[0]


def filename_to_page_no(x):
    if x.startswith(('2021', '2022', '2023', 'westzone')):
        if x.count('_') >= 2:
            return int(x.split('_')[1]) - 1
        elif x.count('_') == 1:
            return int(x.split('_')[-1])

    if x.startswith(('10', 'Mars', 'Panda', 'DARK_STORE', 'MT_TEST', 'PHARMA_TEST', 'PO-', 'SharjahCoopSample',
                     'TAMIMI_VAR', 'TM', 'G0401')):
        return int(x.split('_')[-1])
    if x.startswith(('PO ID', 'Saffola', 'supr', 'G040')):
        return int(x.split('_')[1]) - 1
    if x.startswith(('222', 'coop', 'Marico', 'marico', 'PO', 'Purchase', 'spin', 'Spinneys')):
        return int(x.split('_')[-2]) - 1
    if x.startswith(('G040', 'nesto', 'Nesto')):
        return int(x.split('_')[1])
    return int(x.split('_')[-1]) - 1


def get_page_no(df):
    ls = []
    for x, y in zip(df['filename'], df['doc_id']):
        val = x.replace(y, '').split('_')[1]
        ls.append(val)
    return ls


def get_png_jpg_conversion_ls(df):
    doc_id_to_png_or_jpg = create_dict_png_jpg(df, 'doc_id')
    return df['doc_id'].apply(lambda x: doc_id_to_png_or_jpg[x])


def get_method_conversion_ls(df):
    doc_id_to_methods = create_dict_coversion_method(df, 'doc_id')
    return df['doc_id'].apply(lambda x: doc_id_to_methods[x])


def create_df(local_dir):
    image_ls = os.listdir(f'{local_dir}images/train/')
    image_ls = [x[:-4] for x in image_ls]
    df = pd.DataFrame(image_ls, columns=['filename'])
    df['doc_id'] = df['filename'].apply(filename_to_doc_id)
    df['page_no'] = df['filename'].apply(filename_to_page_no)
    df.loc[df['doc_id'].isin(PHARMA_PO_1INDEX), 'page_no'] -= 1
    df['png_or_jpg'] = get_png_jpg_conversion_ls(df)
    df['conversion_methods'] = get_method_conversion_ls(df)
    df.to_csv('png_jpg_pdf2image_lib_data.csv', index=False)


def download_and_get_pdf_path(doc_id, extenstion):
    event = {
        "bucket": "email-attachment-dev",
        "key": f'Attachments/{doc_id}.{extenstion}',
        "region": "ap-south-1"
    }
    return download_from_s3(event['key'], event['bucket'], event['region'])


def copy_original(df_new, old_dir, new_dir):
    for filename in df_new['filename']:
        shutil.copyfile(f'{old_dir}/images/train/{filename}.png', f'{new_dir}/images/train/{filename}.png')
        shutil.copyfile(f'{old_dir}/labels/train/{filename}.txt', f'{new_dir}/labels/train/{filename}.txt')


def get_pdf_path(doc_id):
    pdf_path, is_found = download_and_get_pdf_path(doc_id, 'PDF')
    if not is_found:
        pdf_path, is_found = download_and_get_pdf_path(doc_id, 'pdf')
    return pdf_path, is_found


def use_method_pdf2image(pdf_path, extension):
    return convert_from_path(pdf_path, fmt=extension, output_folder=TEMP_IMAGE_DIR, thread_count=4, paths_only=True,
                             output_file='')


def use_method_pyvips(pdf_path, extension):
    image = pyvips.Image.new_from_file(pdf_path)
    image_paths = []
    for i in range(image.get('n-pages')):
        image = pyvips.Image.new_from_file(pdf_path, page=i)
        image.write_to_file(f"{TEMP_IMAGE_DIR}pyvips-{i}.{extension}")
        image_paths.append(f"{TEMP_IMAGE_DIR}pyvips-{i}.{extension}")
    return image_paths


def use_method_pypdfium(pdf_path, extension):
    pdf = pdfium.PdfDocument(pdf_path)
    n_pages = len(pdf)
    image_paths = []
    for page_number in range(n_pages):
        page = pdf.get_page(page_number)
        pil_image = page.render(scale=3).to_pil()
        pil_image.save(f"{TEMP_IMAGE_DIR}pypdfium-{page_number}.{extension}")
        image_paths.append(f"{TEMP_IMAGE_DIR}pypdfium-{page_number}.{extension}")
    return image_paths


def use_method_pymupdf(pdf_path, extension):
    image_paths = []
    doc = fitz.open(pdf_path)  # open document
    for i, page in enumerate(doc):  # iterate through the pages
        pix = page.get_pixmap()  # render page to an image
        pix.save(f"{TEMP_IMAGE_DIR}pymupdf-{i}.{extension}")
        image_paths.append(f"{TEMP_IMAGE_DIR}pymupdf-{i}.{extension}")
    return image_paths


def use_method_mutool(pdf_path, extension):
    os.system(
        f"mutool convert -o {TEMP_IMAGE_DIR}mutool-%d.{extension} {pdf_path} 1-100")
    image_paths = []
    for i in range(1, 100):
        image_paths.append(f'{TEMP_IMAGE_DIR}mutool-{i}.{extension}')
    return image_paths


def get_image_paths_local(method, pdf_path, extension):
    image_paths_local = []
    if method == 'pdf2image':
        image_paths_local = use_method_pdf2image(pdf_path, extension)
    if method == 'pyvips':
        image_paths_local = use_method_pyvips(pdf_path, extension)
    if method == 'pypdfium':
        image_paths_local = use_method_pypdfium(pdf_path, extension)
    if method == 'pymupdf':
        image_paths_local = use_method_pymupdf(pdf_path, extension)
    if method == 'mutool':
        image_paths_local = use_method_mutool(pdf_path, extension)
    return image_paths_local


def main_working(df, old_dir, new_dir):
    for doc_id in tqdm(df['doc_id'].unique()[40:]):
        df_new = df[df['doc_id'] == doc_id]
        method_type = df_new['conversion_methods'].unique()[0].split(',')
        extension = df_new['png_or_jpg'].unique()[0]

        try:
            pdf_path, is_found = get_pdf_path(doc_id)
            if not is_found:
                print(f'PDF not found for = {doc_id}')
                copy_original(df_new, old_dir, new_dir)
                continue

            for method in method_type:
                image_paths_local = get_image_paths_local(method, pdf_path, extension)

                for filename, page_no in zip(df_new['filename'], df_new['page_no']):
                    shutil.move(f'{os.getcwd()}/{image_paths_local[page_no]}',
                              f'{os.getcwd()}/{new_dir}/images/train/{filename}_{method}.{extension}')
                    shutil.copyfile(f'{old_dir}/labels/train/{filename}.txt',
                                    f'{new_dir}/labels/train/{filename}_{method}.txt')

        except:
            print(f'Error in = {doc_id}')
            copy_original(df_new, old_dir, new_dir)


def create_new_data_from_df(df, old_dir, new_dir):
    if not os.path.isdir(new_dir):
        os.makedirs(new_dir)

    # df_1, df_2, df_3, df_4 = np.array_split(df, 4)
    # multiprocessing_handler(main_working, [(df_1, old_dir, new_dir), (df_2, old_dir, new_dir), (df_3, old_dir, new_dir),
    #                                        (df_4, old_dir, new_dir)])
    main_working(df, old_dir, new_dir)


s3_source_dir = f's3://document-ai-training-data/training_data/table_localisation/column/base_data/'
destination_dir = f'yolov8_column/'
# s3_sync(s3_source_dir, destination_dir)

create_df(destination_dir)

old_dir = f'yolov8_column'
new_dir = f'png_jpg_data'
os.makedirs(f'{os.getcwd()}/{new_dir}/images/train/', exist_ok=True)
os.makedirs(f'{os.getcwd()}/{new_dir}/labels/train/', exist_ok=True)
df = pd.read_csv('png_jpg_pdf2image_lib_data.csv')
create_new_data_from_df(df, old_dir, new_dir)
