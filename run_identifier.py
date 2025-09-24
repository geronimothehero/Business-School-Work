from company_identifier import canonicalize

if __name__ == "__main__":
    df = canonicalize("companies.csv")
    print(df)