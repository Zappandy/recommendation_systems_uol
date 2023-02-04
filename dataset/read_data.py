import gzip
import json
import pandas as pd
pd.options.display.float_format = '{:,}'.format

def read_data(file):

    data = []
    with gzip.open(file) as fin:
        for l in fin:
            d = json.loads(l)
            data.append(d)
    
    return data


def save_data(dataframe, file, size=1000):


    dataframe = dataframe.sample(size)
    dataframe.to_csv(file, index=False)





file = './dataset/Smaller/goodreads_books_children.json.gz'
# lang_code: filter out other langs except en, text_reviews_count, average_rating, description, authors, publication_year, book_id, ratings_count, work_id, title, title_without_series 
# isbn, isbn_13, image_url to generate output for recommendations  
books = read_data(file)
file = './dataset/Smaller/goodreads_reviews_children.json.gz'
# user_id, book_id, review_id, rating, review_text
reviews = read_data(file)
file = './dataset/Smaller/goodreads_interactions_children.json.gz'
# not using it atm
# interactions = read_data(file)
            
books = pd.DataFrame(books)
reviews = pd.DataFrame(reviews)
# interactions = pd.DataFrame(interactions)

# print(set(books.language_code.unique().tolist()))

booksFiltered = books[books['language_code'].isin(['eng', 'en-US', 'en-CA','en-GB', 'en'])]
booksFiltered = booksFiltered[['language_code', 'text_reviews_count', 'average_rating', 'description', 'authors', 'publication_year', 
                                    'book_id', 'ratings_count', 'work_id', 'title', 'title_without_series', 'isbn', 'isbn13', 'image_url' ]]

reviewsFiltered = reviews[['user_id', 'book_id', 'review_id', 'rating', 'review_text']]

booksFiltered = booksFiltered.dropna()
reviewsFiltered = reviewsFiltered.dropna()

print(booksFiltered.shape[0])
print(reviewsFiltered.shape[0])

# print(set(booksFiltered.language_code.unique().tolist()))
booksFiltered = booksFiltered.sample(501, random_state=43)

reviewsFiltered = reviewsFiltered[reviewsFiltered['book_id'].isin(booksFiltered.book_id.array)]

booksFiltered = booksFiltered.dropna()
reviewsFiltered = reviewsFiltered.dropna()

print(booksFiltered.shape)
print(reviewsFiltered.shape)

booksFiltered.to_csv('books.csv', index=False)
reviewsFiltered.to_csv('reviews.csv', index=False)





# print(len(set(idsB).intersection(set(idsR))))



