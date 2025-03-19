from worldquant.entity import Data_Category as Category, Data_Subcategory as Subcategory


def get_or_create_category(session, category_name):
    category = session.query(Category).filter_by(name=category_name).first()
    if category is None:
        category = Category(name=category_name)
        session.add(category)
        session.commit()
    return category


def get_or_create_subcategory(session, subcategory_name):
    subcategory = session.query(Subcategory).filter_by(name=subcategory_name).first()
    if subcategory is None:
        subcategory = Subcategory(name=subcategory_name)
        session.add(subcategory)
        session.commit()
    return subcategory
